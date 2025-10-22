import sys
import json
import argparse
import sqlite3
import multiprocessing as mp
from func_timeout import func_timeout, FunctionTimedOut
from tqdm import tqdm



def load_json(dir):
    with open(dir, 'r') as j:
        contents = json.loads(j.read())
    return contents


def execute_sql(predicted_sql, ground_truth, db_path):
    str_e = ""
    try:
        conn = sqlite3.connect(db_path)
        # Connect to the database
        cursor = conn.cursor()
        cursor.execute(predicted_sql)
        predicted_res = cursor.fetchall()
    except sqlite3.Error as e1:
        predicted_res = e1
        str_e = e1
        print(f"e1:{e1}")
    try:
        cursor.execute(ground_truth)
        ground_truth_res = cursor.fetchall()
    except sqlite3.Error as e2:
        ground_truth_res = e2
        print(e2)

    res = 0
    try:
        if set(predicted_res) == set(ground_truth_res):
            res = 1
    except Exception as e:
        res = 0
        predicted_res = str_e
    return res, predicted_res


def execute_model(predicted_sql, ground_truth, db_place, idx, meta_time_out):

    res, xyr_res = execute_sql(predicted_sql, ground_truth, db_place)
    result = {'sql_idx': idx, 'res': res}
    return result, xyr_res


def package_sqls(sql_path, db_root_path, mode='gpt', data_mode='dev'):
    clean_sqls = []
    db_path_list = []
    if mode == 'gpt':
        sql_data = json.load(
            open(sql_path, 'r'))
        for idx, sql_str in sql_data.items():
            if type(sql_str) == str:
                sql, db_name = sql_str.split('\t----- bird -----\t')
            else:
                sql, db_name = " ", "financial"
            clean_sqls.append(sql)
            db_path_list.append(db_root_path + db_name +
                                '/' + db_name + '.sqlite')

    elif mode == 'gt':
        sqls = open(sql_path)
        sql_txt = sqls.readlines()
        # sql_txt = [sql.split('\t')[0] for sql in sql_txt]
        for idx, sql_str in enumerate(sql_txt):
            sql, db_name = sql_str.strip().split('\t')
            clean_sqls.append(sql)
            db_path_list.append(db_root_path + db_name +
                                '/' + db_name + '.sqlite')

    return clean_sqls, db_path_list


def run_sqls_parallel(exec_result, xyr_result, sqls, db_places, num_cpus=1, meta_time_out=30.0):
    for i, sql_pair in tqdm(enumerate(sqls)):

        predicted_sql, ground_truth = sql_pair
        result, xyr_res = execute_model(predicted_sql, ground_truth,
                                        db_places[i], i, meta_time_out)
        exec_result.append(result)
        xyr_result.append(xyr_res)
    return exec_result,xyr_result


def sort_results(list_of_dicts):
    return sorted(list_of_dicts, key=lambda x: x['sql_idx'])


def compute_acc_by_diff(exec_results, diff_json_path):
    num_queries = len(exec_results)
    results = [res['res'] for res in exec_results]
    contents = load_json(diff_json_path)
    simple_results, moderate_results, challenging_results = [], [], []
    for i, content in enumerate(contents):
        if content['difficulty'] == 'simple':
            simple_results.append(exec_results[i])

        if content['difficulty'] == 'moderate':
            moderate_results.append(exec_results[i])

        if content['difficulty'] == 'challenging':
            challenging_results.append(exec_results[i])

    simple_acc = sum([res['res'] for res in simple_results])/len(simple_results)  if len(simple_results) != 0 else 0
    moderate_acc = sum([res['res'] for res in moderate_results])/len(moderate_results) if len(moderate_results) != 0 else 0
    challenging_acc = sum( [res['res'] for res in challenging_results])/len(challenging_results) if len(challenging_results) != 0 else 0
    all_acc = sum(results)/num_queries
    count_lists = [len(simple_results), len(moderate_results),
                   len(challenging_results), num_queries]
    return simple_acc * 100, moderate_acc * 100, challenging_acc * 100, all_acc * 100, count_lists


def print_data(score_lists, count_lists):
    levels = ['simple', 'moderate', 'challenging', 'total']
    print("{:20} {:20} {:20} {:20} {:20}".format("", *levels))
    print("{:20} {:<20} {:<20} {:<20} {:<20}".format('count', *count_lists))

    print('======================================    ACCURACY    =====================================')
    print("{:20} {:<20.2f} {:<20.2f} {:<20.2f} {:<20.2f}".format(
        'accuracy', *score_lists))


def eval_ex(predicted_sql_path, ground_truth_path, diff_json_path, db_root_path):
    exec_result = []
    xyr_result = []
    pred_queries, db_paths = package_sqls(predicted_sql_path, db_root_path, mode='gpt',
                                          data_mode='dev')
    # generate gt sqls:
    gt_queries, db_paths_gt = package_sqls(ground_truth_path, db_root_path, mode='gt',
                                           data_mode='dev')

    query_pairs = list(zip(pred_queries, gt_queries))
    exec_result,xyr_result = run_sqls_parallel(exec_result, xyr_result, query_pairs, db_places=db_paths,
                      num_cpus=1, meta_time_out=10000)
    exec_result = sort_results(exec_result)

    print('start calculate')
    simple_acc, moderate_acc, challenging_acc, acc, count_lists = \
        compute_acc_by_diff(exec_result, diff_json_path)
    score_lists = [simple_acc, moderate_acc, challenging_acc, acc]
    print_data(score_lists, count_lists)
    print('===========================================================================================')
    print("Finished evaluation")


def eval_ex_and_write_result(predicted_sql_path, ground_truth_path, diff_json_path, db_root_path, output_path):
    exec_result = []
    xyr_result = []
    pred_queries, db_paths = package_sqls(predicted_sql_path, db_root_path, mode='gpt',
                                          data_mode='dev')
    # generate gt sqls:
    gt_queries, db_paths_gt = package_sqls(ground_truth_path, db_root_path, mode='gt',
                                           data_mode='dev')

    query_pairs = list(zip(pred_queries, gt_queries))
    exec_result,xyr_result = run_sqls_parallel(exec_result, xyr_result,query_pairs, db_places=db_paths,
                                        num_cpus=1, meta_time_out=10000)
    # xyr记录查询结果
    file_path = output_path
    with open(file_path, mode='w') as f:
        # 记录包含执行分数的
        # assert len(exec_result)==len(xyr_result)
        # for index,i in enumerate(xyr_result):
        #     f.write(str(exec_result[index])+','+str(i) + '\n')
        for index,i in enumerate(xyr_result):
            f.write(str(i) + '\n')
    exec_result = sort_results(exec_result)

    print('start calculate')
    simple_acc, moderate_acc, challenging_acc, acc, count_lists = \
        compute_acc_by_diff(exec_result, diff_json_path)
    score_lists = [simple_acc, moderate_acc, challenging_acc, acc]
    print_data(score_lists, count_lists)
    print('===========================================================================================')
    print("Finished evaluation")

