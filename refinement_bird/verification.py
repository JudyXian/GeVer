import json
import os
import sys
dir_b_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'util'))
sys.path.append(dir_b_path)
from verification_03_30_util import *
import random
from sql2nl_03_30 import *
from sql_metadata import Parser 
from revision_10_30 import get_filtered_schemas_with_description_new
# import numpy as np
import argparse


#TODO
def verification_self_consistency(input_path, output_path):
    #根据 self-consistency 机制进行验证   输出precision  recall  f1 等指标
    with open(input_path, 'r') as self_f:
        all_data = json.load(self_f)
    
    all_results = []
    pred_results = []
    pred_norm_results = []
    pred_merge_results = []
    for one_data in all_data:
        difficulty = one_data['difficulty']
        question = one_data['question']
        if difficulty == 'challenging':
            if 'pred_all_list' in one_data.keys():
                true_ground = one_data['ground_truth_res_hash'][0] # true_ground 这里存的是list
                true_norm_ground = one_data['normalized_gold_res_hash'][0]
                true_res = one_data['exec_score']['res']
                all_results.append(true_res)
                pred_all_dict = one_data['pred_all_list']
                ground_truth_res_hash_to_sql = dict()
                normalized_gold_res_hash_to_sql = dict()
                for _, sqls in pred_all_dict.items(): #对于多database instance的情况
                    for one_sql, ver in sqls.items():
                        # print("for test")
                        if ver[1] not in ground_truth_res_hash_to_sql.keys():
                            ground_truth_res_hash_to_sql[ver[1]] = 0
                        ground_truth_res_hash_to_sql[ver[1]] += 1
                        if ver[2] not in normalized_gold_res_hash_to_sql.keys():
                            normalized_gold_res_hash_to_sql[ver[2]] = 0
                        normalized_gold_res_hash_to_sql[ver[2]] += 1
                # 找到结果出现最多的
                max_ground_truth_res_hash_to_sql  = 0
                max_ground_truth_res_hash_to_sql_list = []
                max_normalized_gold_res_hash_to_sql = 0
                max_normalized_gold_res_hash_to_sql_list = []

                for g_res, g_num in ground_truth_res_hash_to_sql.items():
                    if g_num > max_ground_truth_res_hash_to_sql:
                        max_ground_truth_res_hash_to_sql = g_num
                        max_ground_truth_res_hash_to_sql_list.clear()
                        max_ground_truth_res_hash_to_sql_list.append(g_res)
                    elif g_num == max_ground_truth_res_hash_to_sql:
                        max_ground_truth_res_hash_to_sql_list.append(g_res)
                for n_res, n_num in normalized_gold_res_hash_to_sql.items():
                    if n_num > max_normalized_gold_res_hash_to_sql:
                        max_normalized_gold_res_hash_to_sql = n_num
                        max_normalized_gold_res_hash_to_sql_list.clear()
                        max_normalized_gold_res_hash_to_sql_list.append(n_res)
                    elif n_num == max_normalized_gold_res_hash_to_sql:
                        max_normalized_gold_res_hash_to_sql_list.append(n_res)
                if len(ground_truth_res_hash_to_sql) == 0:
                    print(pred_all_dict)
                    print(question)
                # print(f"len of ground_truth_res_hash_to_sql:{len(ground_truth_res_hash_to_sql)}")
                # print(f"len of max_ground_truth_res_hash_to_sql_list: {len(max_ground_truth_res_hash_to_sql_list)}")
                ret_max_ground_truth_res_hash_to_sql_list = random.choice(max_ground_truth_res_hash_to_sql_list)
                ret_max_normalized_gold_res_hash_to_sql_list = random.choice(max_normalized_gold_res_hash_to_sql_list)
                # print(pred_all_dict)
                pred_results.append(1 if ret_max_ground_truth_res_hash_to_sql_list == true_ground else 0)
                pred_norm_results.append(1 if ret_max_normalized_gold_res_hash_to_sql_list == true_norm_ground else 0)
                pred_merge_results.append(1 if (ret_max_ground_truth_res_hash_to_sql_list == true_ground or ret_max_normalized_gold_res_hash_to_sql_list == true_norm_ground) else 0)
    # print(f"pred_results:{pred_results}")
    # print(f"pred_norm_results:{pred_norm_results}")
    # print(f"all_results:{all_results}")
    precision, recall, f1, tp = precision_recall_f1(pred_results, all_results)
    print(f"precision:{precision}, recall:{recall}, f1:{f1}, tp:{tp}")
    norm_precision, norm_recall, norm_f1, norm_tp = precision_recall_f1(pred_norm_results, all_results)
    print(f"norm_precision:{norm_precision}, norm_recall:{norm_recall}, norm_f1:{norm_f1}, norm_tp:{norm_tp}")
    merge_precision, merge_recall,merge_f1, merge_tp = precision_recall_f1(pred_merge_results, all_results)
    print(f"merge_precision:{merge_precision}, merge_recall:{merge_recall}, merge_f1:{merge_f1}, merge_tp:{merge_tp}")



'''
output_sql2nl_path: sql2nl结果
input_path: 问题分解后的结果

output_verify_path / output_verify_path_increment 验证输出 -->只输出语义一致性判断结果   这里不是最终的验证
'''
def verification(input_path, sl_path, output_sql2nl_path, output_verify_path, output_verify_path_increment, is_chess = True, is_direct = False, model_name='deepseek-chat'):
    with open(input_path, 'r') as all_f:
        all_data = json.load(all_f)

    linking_infos = []
    with open(sl_path, 'r') as sl_f: #与问题相关的schema信息  只有challenging的问题
        linking_infos = json.load(sl_f)

    with open(output_sql2nl_path, 'r') as sql2nl_f: #sql2nl的解释信息
        sql2nl_data = json.load(sql2nl_f)

    print(f"len of all_data:{len(all_data)}")
    print(f"len of linking_infos:{len(linking_infos)}")

    all_similar_values_all = []  #这个可能没用 先保留着吧
    with open('all_similar_values_for_bird.json','r') as f_similar:  # 所有数据
        for line in f_similar.readlines():
            if line.strip():
                all_similar_values_all.append(json.loads(line)) #schemas 

    ques_index = 0
    linking_infos_index = 0
    sql2nl_output_data = []
    total_verifying_sqls = 0
    all_results = []
    pred_results = []
    fine_grained_all_results = []
    fine_grained_pred_results = []
    for one_data in all_data:
        difficulty = one_data['difficulty']
        if difficulty == 'challenging' and 'pred_all_list' in one_data.keys():

            question = one_data['question']
            db_id = one_data['db_id']

            one_sql2nl_data = sql2nl_data[linking_infos_index]
            if not (one_sql2nl_data['db_id'] == db_id  and one_sql2nl_data['question'] == question):
                print(f"question and db id cannot match:\n db_id: {db_id};{one_sql2nl_data['db_id']}.  question:{question};{one_sql2nl_data['question']}")
                exit(-1)

            hint = one_data['evidence']
            similar_values = all_similar_values_all[ques_index]
            one_linking_infos = linking_infos[linking_infos_index]

            true_res = one_data['exec_score']['res']
            all_results.append(true_res)
        
            filter_similar_values, schemas, filtered_tables = get_filtered_schemas_with_description_new(question, similar_values, one_linking_infos, db_id, is_chess=is_chess)

            pred_all_list = None
            if len(one_data['pred_all_list']) == 1:
                for _, one_pred_all_list in one_data['pred_all_list'].items():
                    pred_all_list = one_pred_all_list
            else:
                print(f"cannot produce multiple tables")
                exit(-1)

            one_sql2nl_output_data = {}
            one_sql2nl_output_data['db_id'] = db_id
            one_sql2nl_output_data['question'] = question
            one_sql2nl_output_data['hint'] = hint
            one_sql2nl_output_data['pred_all_list'] = []
            one_sql2nl_output_data['true_result'] = true_res

            pred_all_list_one_sql2nl_data = one_sql2nl_data['pred_all_list']
            pred_all_list_index = 0
            for one_predicted, one_value in pred_all_list.items():
                # one_predicted 对应 sql       one_value对应执行结果 
                if one_predicted != "" and "select" in one_predicted.lower().strip():
                    print(f"current verify sql index:{total_verifying_sqls}")
                    total_verifying_sqls += 1
                    tmp_sql2nl_output_data = {}
                    one_predicted_sql2nl_data = pred_all_list_one_sql2nl_data[pred_all_list_index]  #对应的sql2nl 结果
                    pred_all_list_index += 1
                    # sql_explanation 从文件中读取
                    sql_explanation = one_predicted_sql2nl_data['sql_explanation']

                    if one_predicted_sql2nl_data['one_predicted'] != one_predicted:  #看起来字符串比较是可以的
                        print(f"one_predicted does not match:\n {one_predicted}---\n{one_predicted_sql2nl_data['one_predicted']}")
                        exit(-1)

                    tmp_sql2nl_output_data['one_predicted'] = one_predicted
                    tmp_sql2nl_output_data['true_ans'] = one_value[0]  #真实执行结果 --->其实执行不执行应该都可以   这个值只是用来计算f1等metric的
                    fine_grained_all_results.append(one_value[0])
                    tmp_sql2nl_output_data['sql_explanation'] = sql_explanation

                    prompt_sql_explanation = "" #把生成的explanation解析成自然语言
                    
                    if not is_direct:
                        # 构造 prompt_sql_explanation
                        if type(sql_explanation) == str:
                            prompt_sql_explanation = sql_explanation
                        elif type(sql_explanation) == dict and 'error' in sql_explanation.keys():
                            prompt_sql_explanation = direct_generate_nl_by_llm(one_predicted) #用STEPS无法生成 explanation 就直接用大模型生成
                            tmp_sql2nl_output_data['error_generated'] = prompt_sql_explanation
                        elif type(sql_explanation) == dict and len(sql_explanation) > 1:
                            #有多个子句
                            prompt_sql_explanation += "There are multiple sub-queries. Each sub-query has an alias name excepting the final one. When explaining them, their alias is mentioned first.\n\n"
                            table_index = 0
                            for tb, explain in sql_explanation.items():
                                # 这里每个 explain也是个list
                                if table_index < len(sql_explanation) - 1:
                                    prompt_sql_explanation += f"Table: {tb}\n"
                                else:
                                    prompt_sql_explanation += f"The final query:\n"
                                for each_sub_explain in explain:
                                    prompt_sql_explanation += (each_sub_explain['number']+"\n")
                                    for one_explain in each_sub_explain['explanation']:
                                        prompt_sql_explanation += (one_explain['llm_explanation']+"\n")  #不再用  explanation 这个字段
                                    if each_sub_explain['supplement'] != "":
                                        prompt_sql_explanation += (each_sub_explain['supplement']+"\n")
                                prompt_sql_explanation += '\n'
                                table_index += 1
                        else:#没有with 子句
                            for tb, explain in sql_explanation.items():
                                for each_sub_explain in explain:
                                    prompt_sql_explanation += (each_sub_explain['number']+"\n")
                                    for one_explain in each_sub_explain['explanation']:
                                        prompt_sql_explanation += (one_explain['llm_explanation']+"\n")  #不再用  explanation 这个字段
                                    if  each_sub_explain['supplement'] != "":
                                        prompt_sql_explanation += ("\n"+each_sub_explain['supplement']+"\n")
                                prompt_sql_explanation += '\n'
                    else:
                        prompt_sql_explanation = sql_explanation
                        
                    #不格式化了  用原始的sql
                    consistency, feedback, input_cost, output_cost = prompt_verify(question, hint, one_predicted, prompt_sql_explanation, schemas, model_name=model_name)
                    print(f"results:\n{consistency, feedback}")
                    print(f"true result:{tmp_sql2nl_output_data['true_ans']}")
                    tmp_sql2nl_output_data['consistency'] = consistency
                    tmp_sql2nl_output_data['feedback'] = feedback
                    tmp_sql2nl_output_data['consistency_input_cost'] = input_cost
                    tmp_sql2nl_output_data['consistency_output_cost'] = output_cost
                    one_sql2nl_output_data['pred_all_list'].append(tmp_sql2nl_output_data)
                    if 'yes' in consistency.lower():
                        fine_grained_pred_results.append(1)
                    else:
                        fine_grained_pred_results.append(0)
                else:
                    print(f"error predicted sql:{one_predicted}")
            linking_infos_index+=1
            sql2nl_output_data.append(one_sql2nl_output_data)
            increment_write_path(output_verify_path_increment, one_sql2nl_output_data)
        ques_index+=1

    print(f"total_verifying_sqls:{total_verifying_sqls}")
    print(f"pred_results:{pred_results}")
    print(f"all_results:{all_results}")
    precision, recall, f1, tp = precision_recall_f1(pred_results, all_results)
    print(f"precision:{precision}, recall:{recall}, f1:{f1}, tp:{tp}")

    print(f"fine_grained_pred_results:{fine_grained_pred_results}")
    print(f"fine_grained_all_results:{fine_grained_all_results}")
    fgprecision, fgrecall, fgf1, fgtp = precision_recall_f1(fine_grained_pred_results, fine_grained_all_results)
    print(f"fine grained results. precision:{fgprecision}, recall:{fgrecall}, f1:{fgf1}, tp:{fgtp}")

    with open(output_verify_path, 'w') as outpu_f:
        json.dump(sql2nl_output_data, outpu_f, indent=4)


def sql2nl_generation(input_path, sl_path, output_sql2nl_path, output_sql2nl_path_increment, table_path = "", is_direct_generate=False):
    with open(input_path, 'r') as all_f: #分解后 的评估结果文件
        all_data = json.load(all_f)

    table_schemas = None
    if table_path != "":
        with open(table_path, 'r') as table_f:
            table_schemas = json.load(table_f)

    linking_infos = []
    with open(sl_path, 'r') as sl_f:
        linking_infos = json.load(sl_f)

    print(f"len of all_data:{len(all_data)}")
    print(f"len of linking_infos:{len(linking_infos)}")

    all_similar_values_all = []  #这个可能没用 先保留着吧
    with open('all_similar_values_for_bird.json','r') as f_similar:  # 所有数据
        for line in f_similar.readlines():
            if line.strip():
                all_similar_values_all.append(json.loads(line)) #schemas 

    ques_index = 0
    linking_infos_index = 0
    sql2nl_output_data = []
    total_verifying_sqls = 0
    for one_data in all_data:
        difficulty = one_data['difficulty']
        if difficulty == 'challenging' and 'pred_all_list' in one_data.keys():
            #pred_all_list 是生成的所有SQL结果?? --->应该是evaluation 执行的结果
            question = one_data['question']
            db_id = one_data['db_id']

            one_db_schema = None
            for one_schema in table_schemas:
                if one_schema['db_id'] == db_id:
                    one_db_schema = one_schema
        
            pred_all_list = None
            if len(one_data['pred_all_list']) == 1: #只有一个database的情况，所以输入文件必须是EX产生的结果
                for _, one_pred_all_list in one_data['pred_all_list'].items():
                    pred_all_list = one_pred_all_list
            else:
                print(f"cannot produce multiple tables")
                exit(-1)

            one_sql2nl_output_data = {}
            one_sql2nl_output_data['db_id'] = db_id
            one_sql2nl_output_data['question'] = question
            one_sql2nl_output_data['pred_all_list'] = []
            for one_predicted, one_value in pred_all_list.items():
                if one_predicted != "" and "select" in one_predicted.lower().strip():
                    # one_predicted 开始对 one_predicted 进行verification
                    # 先做 sql2nl   每个sql都生成一遍
                    print(f"current verify sql index:{total_verifying_sqls}")
                    total_verifying_sqls += 1
                    # for test 
                    # if total_verifying_sqls > 10:
                    #     continue
                    tmp_sql2nl_output_data = {}
                    tmp_sql2nl_output_data['one_predicted'] = one_predicted
                    if not is_direct_generate:
                        try:
                            sql_explanation = sql2nl(one_predicted, one_db_schema)
                        except Exception as e:
                            sql_explanation = direct_generate_nl_by_llm(one_predicted)
                    else:
                        sql_explanation = direct_generate_nl_by_llm_without_step(one_predicted)  
                    tmp_sql2nl_output_data['sql_explanation'] = sql_explanation

                    one_sql2nl_output_data['pred_all_list'].append(tmp_sql2nl_output_data)
                else:
                    print(f"error predicted sql:{one_predicted}")
                    one_sql2nl_output_data['pred_all_list'].append(f"error predicted sql:{one_predicted}")
            linking_infos_index+=1
            increment_write_path(output_sql2nl_path_increment, one_sql2nl_output_data) 
            sql2nl_output_data.append(one_sql2nl_output_data)

        ques_index+=1

    print(f"total_verifying_sqls:{total_verifying_sqls}")
    with open(output_sql2nl_path, 'w') as output_f:
        json.dump(sql2nl_output_data, output_f, indent=4)
    


if __name__ == '__main__':
    pass