import argparse
import json
import os
import pickle
from pathlib import Path
import sqlite3
from tqdm import tqdm
import random
import numpy as np

from .utils.linking_process import SpiderEncoderV2Preproc
from .utils.pretrained_embeddings import GloVe
from .utils.datasets.spider import load_tables
from .utils.utils import sql2skeleton, jaccard_similarity
from .utils.linking_utils.application import mask_question_with_schema_linking

# from dataset.process.preprocess_kaggle import gather_questions


def schema_linking_producer(train_data,test_data,table, db, dataset_dir,save_path):

    # load schemas
    schemas, eval_foreign_key_maps = load_tables([os.path.join(dataset_dir, table)])

    # Backup in-memory copies of all the DBs and create the live connections
    for db_id, schema in tqdm(schemas.items(), desc="DB connections"):
        sqlite_path = Path(dataset_dir) / db / db_id / f"{db_id}.sqlite"
        source: sqlite3.Connection
        with sqlite3.connect(str(sqlite_path)) as source:
            dest = sqlite3.connect(':memory:')
            dest.row_factory = sqlite3.Row
            source.backup(dest)
        schema.connection = dest

    word_emb = GloVe(kind='42B', lemmatize=True)
    linking_processor = SpiderEncoderV2Preproc(dataset_dir,
            min_freq=4,
            max_count=5000,
            include_table_name_in_column=False,
            word_emb=word_emb,
            fix_issue_16_primary_keys=True,
            compute_sc_link=True,
            compute_cv_link=True)

    # build schema-linking
    for data, section in zip([test_data, train_data],['test', 'train']):
        for item in tqdm(data, desc=f"{section} section linking"):
            db_id = item["db_id"]
            schema = schemas[db_id]
            to_add, validation_info = linking_processor.validate_item(item, schema, section)
            if to_add:
                linking_processor.add_item(item, schema, section, validation_info)

    # save
    # linking_processor.save(save_path=save_path)
    train_linked = []
    test_linked = []
    for section, texts in linking_processor.texts.items():
        assert section in ['train','test']
        if section == 'train':
            for text in texts:
                train_linked.append(text)
        elif section == 'test':
            for text in texts:
                test_linked.append(text)
    return train_linked,test_linked

class BasicExampleSelector(object):
    def __init__(self, data, *args, **kwargs):
        # self.data = data
        # self.train_json = self.data.get_train_json()
        self.train_json = data
        self.db_ids = [d["db_id"] for d in self.train_json]
        self.train_questions = [i['question'] for i in data]

    def get_examples(self, question, num_example, cross_domain=False):
        pass

    def domain_mask(self, candidates: list, db_id):
        cross_domain_candidates = [candidates[i] for i in range(len(self.db_ids)) if self.db_ids[i] != db_id]
        return cross_domain_candidates

    def retrieve_index(self, indexes: list, db_id):
        cross_domain_indexes = [i for i in range(len(self.db_ids)) if self.db_ids[i] != db_id]
        retrieved_indexes = [cross_domain_indexes[i] for i in indexes]
        return retrieved_indexes

class EuclideanDistanceQuestionMaskSelector(BasicExampleSelector):
    def __init__(self, data, *args, **kwargs):
        super().__init__(data)

        self.SELECT_MODEL = "sentence-transformers/all-mpnet-base-v2"
        self.mask_token = "<mask>"  # the "<mask>" is the mask token of all-mpnet-base-v2
        self.value_token = "<unk>" # the "<unk>" is the unknown token of all-mpnet-base-v2

        from sentence_transformers import SentenceTransformer
        train_mask_questions = mask_question_with_schema_linking(self.train_json, mask_tag=self.mask_token, value_tag=self.value_token)
        self.bert_model = SentenceTransformer(self.SELECT_MODEL, device="cpu")
        self.train_embeddings = self.bert_model.encode(train_mask_questions)

    def get_examples(self, target, num_example, cross_domain=False):
        target_mask_question = mask_question_with_schema_linking([target], mask_tag=self.mask_token, value_tag=self.value_token)
        target_embedding = self.bert_model.encode(target_mask_question)

        # find the most similar question in train dataset
        from sklearn.metrics.pairwise import euclidean_distances
        distances = np.squeeze(euclidean_distances(target_embedding, self.train_embeddings)).tolist()
        pairs = [(distance, index) for distance, index in zip(distances, range(len(distances)))]

        train_json = self.train_json
        pairs_sorted = sorted(pairs, key=lambda x: x[0])
        top_pairs = list()
        for d, index in pairs_sorted:
            similar_db_id = train_json[index]["db_id"]
            if cross_domain and similar_db_id == target["db_id"]:
                continue
            top_pairs.append((index, d))
            if len(top_pairs) >= num_example:
                break

        return [train_json[index] for (index, d) in top_pairs]
 

def select_main(train_data,target_data):
    # merge two training split of Spider
    spider_dir = "/home3/xianyiran/text2sql/spider"

    # schema-linking between questions and databases for Spider
    # data = {}
    spider_table = 'tables.json'
    spider_db = 'database'
    save_path = ""
    train_linked,test_linked = schema_linking_producer(train_data, target_data,spider_table, spider_db, spider_dir,save_path)

    selector = EuclideanDistanceQuestionMaskSelector(train_linked)

    selected_examples = selector.get_examples(test_linked[0],5)

    return selected_examples

if __name__ == "__main__":
    train_path = "/home3/xianyiran/text2sql/decomposition_and_DAIL/decomposition_example/bridge_from.json"
    test_path = "/home3/xianyiran/text2sql/decomposition_and_DAIL/decomposition_example/test.json"

    with open(train_path,'r') as f:
        train_datas = json.load(f)
    with open(test_path,'r') as f:
        test_datas = json.load(f)

    selected_jsons = select_main(train_datas,test_datas)
    for i in selected_jsons:
        print(i['example'])