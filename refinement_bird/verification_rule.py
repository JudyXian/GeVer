import json
import os
import sys
dir_b_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'util'))
sys.path.append(dir_b_path)
from jzx_utils import *
from sql_metadata import Parser 
from revision_10_30 import get_filtered_schemas_with_description_new_no_desc
from verification_03_30_util import gpt_new, postprocess_for_refinement, direct_generate_nl_by_llm
from verification_rule_03_30_util import *
from sql2nl_03_30 import sql2nl
from verification_03_30 import prompt_verify
import hashlib
import random
from sql_metadata import Parser 
import argparse

OPENAIKEY=''
BASE_URL = 'https://api.openai.com/v1'

# MODEL='deepseek-chat'
DEEPSEEKKEY=''
DEEPSEEK_BASE_URL='https://api.deepseek.com'


def sql_execute(db_name,sql, db_path):
    db_name = db_name
    path = db_path+f"{db_name}/{db_name}.sqlite"
    # print(path)
    conn = sqlite3.connect(path)
    cs = conn.cursor()
    is_error = False
    try:
        cs.execute(sql)
        result = cs.fetchall()
        #结果最多sample sample_num行数据
    except sqlite3.Error as e:
        is_error = True
        result = str(e)
    return is_error, str(result)


def generate_unique_hash(data):
    hash_object = hashlib.sha256(data.encode())
    unique_hash = hash_object.hexdigest()
    return unique_hash



def verify_percentage(question, hint, candidate_sql, multiple_sqls, shcemas, model_name='deepseek-chat'):
    prompt_percentage = '''
#### You are a SQL expert. Please verify if the SQL query has errors about calculating percentage or ratio. We provide the original SQL and additionally list each sub-SQL for easier verification. If you believe there are no errors, output "Yes"; Otherwise, output "No" with corresponding explanations.

## NL Question
{}
## Hint
{}

## Original SQL Query:
{}

{}

## Relevant Database Schema:
{}

There are some tips to assist you in making judgments:
1. When calculating percentages, it is generally better to use COUNT on a column relevant to the question (preferably a primary key column) rather than directly using COUNT(*).


### Output Format:
Answer: Yes or No
Explanation: Please explain your reasoning.
'''
    prompt_percentage = prompt_percentage.format(question, hint, candidate_sql, multiple_sqls, shcemas)
    print(f"prompt_percentage:\n{prompt_percentage}")

    n_repeat = 0
    max_cnt = 15
    percentage_error = ''
    percentage_error_explanation = ''
    while n_repeat < max_cnt:
        try:
            if 'deepseek' in model_name:
                percentage_error, percentage_error_explanation, input_cost, output_cost = gpt_new(prompt_percentage, model_name, DEEPSEEKKEY, DEEPSEEK_BASE_URL)
            elif 'gpt' in model_name:
                percentage_error, percentage_error_explanation, input_cost, output_cost = gpt_new(prompt_percentage, model_name, OPENAIKEY, BASE_URL)
            else:
                print('model error:'+str(model_name))
                exit(-1)
            break
        except Exception as e:
            n_repeat += 1
            print(
                f"Repeat for the {n_repeat} times for exception: {e}", end="\n")
            time.sleep(1)
            continue
    return percentage_error, percentage_error_explanation, input_cost, output_cost


'''
Relevant Database Schema:  do not need description information
'''
def verify_fk_and_table(question, hint, one_sql, multiple_sqls, schemas, filtered_tables, tables_in_sql, model_name='deepseek-chat'):
    prompt_verification = '''
#### You are a SQL expert. Please verify if the SQL query has Schema-Related Errors. We provide the original SQL and additionally list each sub-SQL for easier verification. If you believe there are no errors, output "Yes"; Otherwise, output "No" with corresponding explanations.

## Given some notes that help you do determination:
1. When there are multiple table joins (or the format of 'tab1.col1 IN (SELECT col1 FROM tab2)', i.e., using the 'IN' operator) in SQL, the join columns (or 'IN' operator) must match the primary-foreign key relationships provided in the "Relevant Database Schema". 
If the primary-foreign key relationships are complex, the join columns (or 'IN' operator) may have multiple possibilities; as long as they match any one of the possible cases, it is right.
The join operator is typically INNER JOIN in most cases, but other types, such as CROSS JOIN, are also acceptable if they align with the semantics of the question.
2. Multiple matching columns between two tables are not allowed. For example, 'tab1 JOIN tab2 ON (tab1.col1 = tab2.col1 OR tab1.col2 = tab2.col2)' is not permitted.
3. You must only check for errors about table matching and primary-foreign key relationship; ignore other issues.

## There are some tips:
1. Double negatives imply a positive. For example, available outside of the United States refers to isForeignOnly = 1, so 'not available outside of the United States' means isForeignOnly = 0.

## Original SQL Query:
{}

{}

## Relevant Database Schema:
{}

## Tables must in SQL:
{}

{}


### Output Format:
Answer: Yes or No
Explanation: Please explain your reasoning.
'''
    # prompt_verification = prompt_verification.format(question, hint, one_sql, multiple_sqls, schemas, filtered_tables, tables_in_sql)
    prompt_verification = prompt_verification.format(one_sql, multiple_sqls, schemas, filtered_tables, tables_in_sql)
    print(f"prompt_verification:\n{prompt_verification}")

    n_repeat = 0
    max_cnt = 15
    fk_and_table = ''
    fk_and_table_explanation = ''
    while n_repeat < max_cnt:
        try:
            if 'deepseek' in model_name:
                fk_and_table, fk_and_table_explanation, input_cost, output_cost = gpt_new(prompt_verification, model_name, DEEPSEEKKEY, DEEPSEEK_BASE_URL)
            elif 'gpt' in model_name:
                fk_and_table, fk_and_table_explanation, input_cost, output_cost = gpt_new(prompt_verification, model_name, OPENAIKEY, BASE_URL)
            else:
                print('model error:'+str(model_name))
                exit(-1)
            break
        except Exception as e:
            n_repeat += 1
            print(
                f"Repeat for the {n_repeat} times for exception: {e}", end="\n")
            time.sleep(1)
            continue
    return fk_and_table, fk_and_table_explanation, input_cost, output_cost


column_to_tables = {}
column_to_uniques = {}
table_to_uniques = {}

def preprocess(db_path, dev_path, output_table_path, output_uniques_path, output_table_uniques_path):
    # 列名到 表名的对应关系    --->并且标出哪个列是unique key
    global column_to_tables
    global column_to_uniques
    global table_to_uniques
    if os.path.exists(output_table_path) and os.path.exists(output_uniques_path):
        with open(output_table_path) as output_f:
            column_to_tables = json.load(output_f)
        with open(output_uniques_path) as unique_f:
            column_to_uniques = json.load(unique_f)
        with open(output_table_uniques_path) as tb_unique_f:
            table_to_uniques = json.load(tb_unique_f)
    else:
        db_set = set()
        column_to_tables = {}
        column_to_uniques = {}
        table_to_uniques = {}
        with open(dev_path,'r') as dev_f:
            dev_data = json.load(dev_f)
        for one_data in dev_data:
            db_set.add(one_data['db_id'])
        for one_db in db_set:
            sqlite_path = db_path + one_db +'/' + one_db +'.sqlite'
            table_names = get_table_names_path(sqlite_path)
            #key是column   value是 table name 和 是否unique key

            tmp_column_to_tables = {}
            table_to_uniques[one_db] = {}
            column_to_uniques[one_db] = {}
            for table_name in table_names:
                table_to_uniques[one_db][table_name.lower().strip()] = {}
                schema = get_schema_path(sqlite_path, table_name) 
                primary_keys = get_primary_keys(schema) #unique_keys 全部改成小写的了
                result_type, result = exec_db_path_(sqlite_path, column_type_query % table_name) #table中每个 column的具体信息
                for (
                    columnID, column_name, columnType,
                    columnNotNull, columnDefault, columnPK,
                ) in result:
                    #全部小写
                    lower_column_name = column_name.lower().strip()
                    if lower_column_name not in tmp_column_to_tables.keys():
                        tmp_column_to_tables[lower_column_name] = []
                    tmp_column_to_tables[lower_column_name].append(table_name.lower().strip())
                    if lower_column_name in primary_keys:
                        is_unique = True
                        table_to_uniques[one_db][table_name.lower().strip()][lower_column_name] = True
                    else:
                        is_unique = False
                        table_to_uniques[one_db][table_name.lower().strip()][lower_column_name] = False
                    if lower_column_name not in column_to_uniques[one_db].keys():
                        column_to_uniques[one_db][lower_column_name] = {}
                    column_to_uniques[one_db][lower_column_name][table_name.lower().strip()] = [is_unique, columnType]
            column_to_tables[one_db] = tmp_column_to_tables
        with open(output_table_path, 'w') as output_f:
            json.dump(column_to_tables, output_f, indent=4)
        with open(output_uniques_path,'w') as unique_f:
            json.dump(column_to_uniques, unique_f, indent=4)
        with open(output_table_uniques_path,'w') as unique_tb_f:
            json.dump(table_to_uniques, unique_tb_f, indent=4)


'''
多次 verification and revision
'''
def loop_verification_and_revision(input_total_path, input_path, sl_path, output_path, db_path, loop_num, table_path="", model_name='deepseek-chat'):
    with open(input_path, 'r') as all_f:
        all_data = json.load(all_f)

    with open(sl_path, 'r') as sl_f: #与问题相关的schema信息  只有challenging的问题
        linking_infos = json.load(sl_f)

    with open(input_total_path, 'r') as all_total_f:
        all_total_data = json.load(all_total_f)

    all_similar_values_all = []  #这个可能没用 先保留着吧
    with open('all_similar_values_for_bird.json','r') as f_similar:  # 所有数据
        for line in f_similar.readlines():
            if line.strip():
                all_similar_values_all.append(json.loads(line)) #schemas 
    # prompt需要schema信息
    table_schemas = None
    if table_path != "":
        with open(table_path, 'r') as table_f:
            table_schemas = json.load(table_f)

    linking_infos_index = 0
    ques_index = 0
    for one_total_data in all_total_data:
        if one_total_data['difficulty'] != 'challenging':
            ques_index += 1
            # linking_infos_index+=1
            continue
        if 'chosed_sql_revised' not in all_data[linking_infos_index].keys():
            ques_index += 1
            linking_infos_index += 1
            continue

        if one_total_data['question'] != all_data[linking_infos_index]['question']:
            print("question are not same")
            print(f"one_total_data['question']:{one_total_data['question']}")
            print(f"all_data[linking_infos_index]['question']:{all_data[linking_infos_index]['question']}")
            exit(-1)
        
        #for test 
        if one_total_data['question'] != 'What is the percentage of carcinogenic molecules in triple type bonds?':
            ques_index += 1
            linking_infos_index += 1
            continue

        db_id = all_data[linking_infos_index]['db_id']
        question = all_data[linking_infos_index]['question']
        hint = one_total_data['evidence']
        is_chess = True
        similar_values = all_similar_values_all[ques_index]
        one_linking_infos = linking_infos[linking_infos_index]
        # 只考虑 带 percentage的challenging 问题
        filter_similar_values, schemas_no_desc, filtered_tables = get_filtered_schemas_with_description_new_no_desc(question, similar_values, one_linking_infos, db_id, is_chess=is_chess)
        _, schemas_, _ = get_filtered_schemas_with_description_new_no_desc(question, similar_values, one_linking_infos, db_id, is_chess=is_chess)
        to_revising_sql = all_data[linking_infos_index]['chosed_sql_revised']

        one_db_schema = None
        for one_schema in table_schemas:
            if one_schema['db_id'] == db_id:
                one_db_schema = one_schema

        for i in range(loop_num):
            print(f"current index:{i}  loop_num:{loop_num}")
            all_right_num = 0
            tmp_to_revising_sql = postprocess(to_revising_sql)
            multiple_sqls = transform_multiple_sqls(tmp_to_revising_sql)
            tmp_to_revising_sql = postprocess(to_revising_sql)
            one_sql_parserd = Parser(tmp_to_revising_sql)
            print(f"to_revising_sql 1:{to_revising_sql}")
            tables_in_sql_prompt = ""
            table_matching = True
            try:
                tables_in_sql = one_sql_parserd.tables
                if sorted(filtered_tables) == sorted(tables_in_sql):
                    tables_in_sql_prompt = f"Tables in SQL are:{tables_in_sql}, which are the same as the Tables. Thus, the tables are matching."
                else:
                    table_matching = False
                    tables_in_sql_prompt = f"Tables in SQL are:{tables_in_sql}, which are different from the Tables. Thus, there are errors as the tables are not matching. You must output 'No'."
            except Exception as e:
                print(f"error:{e}")
            verify_res, verify_exp, verify_input_cost, verify_output_cost = verify_fk_and_table(question, hint, to_revising_sql, multiple_sqls, schemas_no_desc, filtered_tables, tables_in_sql_prompt, model_name=model_name)
            if (not table_matching) and verify_res.lower() == 'yes':
                verify_res = 'no'
                verify_exp = 'Tables in SQL are different from the given Tables.'
            if verify_res.lower() == 'no':
                # to_revising_sql = postprocess(to_revising_sql)
                to_revising_sql = revision_schema_error(question, hint, to_revising_sql, verify_exp, schemas_no_desc, filtered_tables, model_name)
            else:
                all_right_num += 1

            # execution error 
            print(f"to_revising_sql 2:{to_revising_sql}")
            tmp_to_revising_sql = postprocess(to_revising_sql)
            is_error, execution_error = sql_execute(db_id, tmp_to_revising_sql, db_path)
            if is_error:
                to_revising_sql = revision_execution_error(question, hint, to_revising_sql, execution_error, schemas_no_desc, model_name)
            else:
                all_right_num += 1

            # database value error
            db_values_list = []
            hash_predicate_no_values = []
            tmp_to_revising_sql = postprocess(to_revising_sql)
            p_sql = Parser(tmp_to_revising_sql)
            parsed_sql = p_sql.query
            comparison_list = []
            is_have_with = True
            have_null_data = False
            try:
                with_name_len = len(p_sql.with_names)
            except Exception as e:
                print(f"e:{e}\nparsed_sql:{parsed_sql}")
                is_have_with = False
            with_tables = []
            if is_have_with and with_name_len > 0 and 'WITH' in parsed_sql.upper():
                print(f"with tables:{p_sql.with_names}")
                for i in range(with_name_len):
                    with_tables.append(p_sql.with_names[i])
                for one_table in with_tables:
                    sub_sql = str(p_sql.with_queries[one_table])
                    print(f"sub_sql:{sub_sql}")
                    tmp_comparison_list = extract_all_comparison(sub_sql)
                    comparison_list.extend(tmp_comparison_list)

                final_sql = re.sub(r"WITH\s+.*?\)\s*(SELECT)", r"\1", parsed_sql, flags=re.IGNORECASE | re.DOTALL)
                final_comparison_list = extract_all_comparison(final_sql)
                comparison_list.extend(final_comparison_list)
            else:
                tmp_comparison_list = extract_all_comparison(tmp_to_revising_sql)
                comparison_list.extend(tmp_comparison_list)

                    #收集完 comparison_list
            parse_comparison_list = [extract_info_from_comparison(c) for c in comparison_list]
            if len(comparison_list) > 0:
                for one_comparison in parse_comparison_list:
                    if 'table_col' in one_comparison.keys() and 'val' in one_comparison.keys():
                        table_col = one_comparison['table_col']
                        extract_column = table_col[1]
                        extract_column = extract_column.lower().strip()
                        extract_val = one_comparison['right'].value

                        if extract_column not in column_to_tables[db_id]:
                            continue
                        extract_tables =  column_to_tables[db_id][extract_column]
                        for one_extract_tb in extract_tables:
                            if (column_to_uniques[db_id][extract_column][one_extract_tb][1] != "TEXT") and ("VARCHAR" not in column_to_uniques[db_id][extract_column][one_extract_tb][1]):
                                continue 
                            select_query = 'SELECT `'+extract_column+'` FROM `'+one_extract_tb+'` WHERE `'+extract_column+'` = '+extract_val
                            sqlite_path = db_path + db_id +'/' + db_id +'.sqlite' 
                            result, datas = exec_db_path_all(sqlite_path, select_query)
                            if result != "result":
                                print(f"error: {datas}  sqlite_path:{sqlite_path}")
                                # exit(-1)
                            else:
                                if len(datas) == 0:
                                    add_data = {"table":one_extract_tb, "column":extract_column, "value":extract_val}
                                    hash_add_data = generate_unique_hash(str(add_data))
                                    if hash_add_data not in hash_predicate_no_values:
                                        db_values_list.append(add_data)
                                        hash_predicate_no_values.append(hash_add_data)
                                    have_null_data = True
                                else: #right
                                    pass
                    else:
                        continue
            if have_null_data:
                predicate_no_values_data_add = []
                for table_values in db_values_list:
                    sql_clause = "SELECT DISTINCT `{}` FROM `{}` LIMIT 3".format(table_values['column'], table_values['table'])
                    result, datas = exec_db_path_(sqlite_path, sql_clause)
                    if result != "result":
                        print(f"error: {datas}  sqlite_path:{sqlite_path}")
                        # exit(-1)
                    predicate_no_values_data_add.append(f"{table_values['table']}.{table_values['column']}:  {datas}")
                to_revising_sql = revision_db_value_error(question, hint, to_revising_sql, predicate_no_values_data_add, filter_similar_values, model_name)
            else:
                all_right_num += 1
            
            if all_right_num == 3: #全部正确 就不再继续revision ---> 去掉percentage相关的
                break
        
        all_data[linking_infos_index]['final_chosed_sql_revised'] = to_revising_sql
        ques_index += 1
        linking_infos_index += 1

        # for test
        if one_total_data['question'] == 'What is the percentage of carcinogenic molecules in triple type bonds?':
            exit(-1)
    with open(output_path, 'w') as out_f: 
        json.dump(all_data, out_f, indent=4)

'''
几种错误都没有就直接输出 
如果没用consistency错误，就看哪个的错误类型最少--->选一个错误类型最少的进行修正
如果有consistency错误，也是选择错误类型最少的进行修正
'''
def process_and_revision(input_total_path, input_path, sl_path, output_path, db_path, model_name = 'deepseek-chat'):
    with open(input_path, 'r') as all_f:
        all_data = json.load(all_f)

    with open(sl_path, 'r') as sl_f: #与问题相关的schema信息  只有challenging的问题
        linking_infos = json.load(sl_f)

    with open(input_total_path, 'r') as all_total_f:
        all_total_data = json.load(all_total_f)

    all_similar_values_all = []  #这个可能没用 先保留着吧
    with open('all_similar_values_for_bird.json','r') as f_similar:  # 所有数据
        for line in f_similar.readlines():
            if line.strip():
                all_similar_values_all.append(json.loads(line)) #schemas 
    # prompt需要schema信息
    linking_infos_index = 0
    ques_index = 0
    for one_total_data in all_total_data:
        if one_total_data['difficulty'] != 'challenging':
            ques_index+=1
            continue
        
        if one_total_data['question'] != all_data[linking_infos_index]['question']:
            print("question are not same")
            print(f"one_total_data['question']:{one_total_data['question']}")
            print(f"all_data[linking_infos_index]['question']:{all_data[linking_infos_index]['question']}")
            exit(-1)

        if 'pred_all_list' not in all_data[linking_infos_index].keys():
            print('no key pred_all_list')
            exit(-1)

        db_id = all_data[linking_infos_index]['db_id']
        sqlite_path = db_path + db_id +'/' + db_id +'.sqlite' 
        question = all_data[linking_infos_index]['question']
        hint = one_total_data['evidence']
        is_chess = True
        similar_values = all_similar_values_all[ques_index]
        one_linking_infos = linking_infos[linking_infos_index]
        filter_similar_values, schemas_no_desc, filtered_tables_ = get_filtered_schemas_with_description_new_no_desc(question, similar_values, one_linking_infos, db_id, is_chess=is_chess)
        pred_all_list = all_data[linking_infos_index]['pred_all_list']

        #第一轮遍历 将所有sql分组:  全对/consistency/no consistency
        all_right_sqls = []
        # all_right_index = []
        consistency_sqls = []
        # consistency_index = []
        no_consistency_sqls = []
        # no_consistency_index = []
        for i in range(len(pred_all_list)): 
            tmp_pred_all_value = pred_all_list[i]
            if tmp_pred_all_value['consistency'] == 'yes':
                if 'predicate_no_value' not in tmp_pred_all_value.keys() and tmp_pred_all_value['fk_and_table'].lower() == 'yes' and 'execution_error' not in tmp_pred_all_value.keys() and ('percentage_error' not in tmp_pred_all_value.keys() or ('percentage_error' in tmp_pred_all_value.keys() and tmp_pred_all_value['percentage_error'].lower() == 'yes')):
                    all_right_sqls.append([tmp_pred_all_value['one_predicted'], i])
                    # all_right_index.append(i)
                else:
                    #还需要记录有几个错误类型 except consistency
                    error_num = 0 
                    if 'predicate_no_value' in tmp_pred_all_value.keys():
                        error_num += 1
                    if  tmp_pred_all_value['fk_and_table'].lower() == 'no':
                        error_num += 1
                    if  'percentage_error' in tmp_pred_all_value.keys() and tmp_pred_all_value['percentage_error'].lower() == 'no':
                        error_num += 1
                    if 'execution_error' in tmp_pred_all_value.keys():
                        error_num += 1
                    consistency_sqls.append([tmp_pred_all_value['one_predicted'], i, error_num])
                    # consistency_index.append(i)
            else:
                error_num = 0
                if 'predicate_no_value' in tmp_pred_all_value.keys():
                    error_num += 1
                if  tmp_pred_all_value['fk_and_table'].lower() == 'no':
                    error_num += 1
                if  'percentage_error' in tmp_pred_all_value.keys() and tmp_pred_all_value['percentage_error'].lower() == 'no':
                    error_num += 1
                if 'execution_error' in tmp_pred_all_value.keys():
                    error_num += 1
                no_consistency_sqls.append([tmp_pred_all_value['one_predicted'], i, error_num + 1])
                # no_consistency_index.append(i)
        #如果all_right_sqls 全为空 就随机选一个直接返回

        if len(all_right_sqls) > 0:
            chosed_sql = random.choice(all_right_sqls)
            selected_one_sql = pred_all_list[chosed_sql[1]]
            all_data[linking_infos_index]['chosed_sql'] = chosed_sql[0]
            if selected_one_sql['true_ans'] == 1:
                all_data[linking_infos_index]['label'] = 'verify all right'
            else:
                all_data[linking_infos_index]['label'] = 'verify right but wrong'
        elif len(consistency_sqls) > 0:
            #对 consistency_sqls 按照最后一项从小到大排序  相同值随机
            sorted_data = sorted(consistency_sqls, key=lambda x: (x[2], random.random()))
            chosed_sql = sorted_data[0]
            all_data[linking_infos_index]['chosed_sql'] = chosed_sql[0]
            selected_one_sql = pred_all_list[chosed_sql[1]]
            if selected_one_sql['true_ans'] == 1:
                all_data[linking_infos_index]['label'] = 'consistency no need revision'
            else:
                all_data[linking_infos_index]['label'] = 'consistency need revision'
            # 需要对chosed_sql 做revision
            #依次修正
            all_data[linking_infos_index]['intermediate_revised_sqls'] = []
            revised_sql = chosed_sql[0]
            if  'percentage_error' in selected_one_sql.keys() and selected_one_sql['percentage_error'].lower() == 'no': #暂时忽略
                feedback_percentage_error = selected_one_sql['percentage_error_feedback']
                revised_sql = revision_percentage_error(question, hint, revised_sql, feedback_percentage_error, schemas_no_desc, model_name)
                all_data[linking_infos_index]['intermediate_revised_sqls'].append(revised_sql)
            if  selected_one_sql['fk_and_table'] == 'no':
                feedback_fk_and_table = selected_one_sql['fk_and_table_feedback']
                revised_sql = revision_schema_error(question, hint, revised_sql, feedback_fk_and_table, schemas_no_desc, filtered_tables_, model_name)
                all_data[linking_infos_index]['intermediate_revised_sqls'].append(revised_sql)
            if 'execution_error' in selected_one_sql.keys():
                feedback_exe_error = selected_one_sql['execution_error']
                revised_sql = revision_execution_error(question, hint, revised_sql, feedback_exe_error, schemas_no_desc, model_name)
                all_data[linking_infos_index]['intermediate_revised_sqls'].append(revised_sql)
            if 'predicate_no_value' in selected_one_sql.keys():
                predicate_no_values_data_add = []
                for table_values in selected_one_sql['predicate_no_value']:
                    sql_clause = "SELECT DISTINCT `{}` FROM `{}` LIMIT 3".format(table_values['column'], table_values['table'])
                    result, datas = exec_db_path_(sqlite_path, sql_clause)
                    if result != "result":
                        print(f"error: {datas}  sqlite_path:{sqlite_path}")
                    predicate_no_values_data_add.append(f"{table_values['table']}.{table_values['column']}:  {datas}")
                revised_sql = revision_db_value_error(question, hint, revised_sql, predicate_no_values_data_add, filter_similar_values, model_name)
                all_data[linking_infos_index]['intermediate_revised_sqls'].append(revised_sql)
            all_data[linking_infos_index]['chosed_sql_revised'] = revised_sql
        else:
            all_data[linking_infos_index]['intermediate_revised_sqls'] = []
            sorted_data = sorted(no_consistency_sqls, key=lambda x: (x[2], random.random()))
            chosed_sql = sorted_data[0]
            all_data[linking_infos_index]['chosed_sql'] = chosed_sql[0]
            if selected_one_sql['true_ans'] == 1:
                all_data[linking_infos_index]['label'] = 'no consistency no need revision'
            else:
                all_data[linking_infos_index]['label'] = 'no consistency need revision'
            # 需要对chosed_sql 做revision
            selected_one_sql = pred_all_list[chosed_sql[1]]
            feedback = selected_one_sql['feedback']
            revised_sql = chosed_sql[0]
            revised_sql = revision_logic_error(question, hint, revised_sql, feedback, schemas_no_desc, model_name)
            all_data[linking_infos_index]['intermediate_revised_sqls'].append(revised_sql)
            if  'percentage_error' in selected_one_sql.keys() and selected_one_sql['percentage_error'].lower() == 'no': #这一项忽略
                feedback_percentage_error = selected_one_sql['percentage_error_feedback']
                revised_sql, input_cost, output_cost = revision_percentage_error(question, hint, revised_sql, feedback_percentage_error, schemas_no_desc, model_name)
                all_data[linking_infos_index]['intermediate_revised_sqls'].append(revised_sql)
                all_data[linking_infos_index]['revision_percentage_error_input_cost'] = input_cost
                all_data[linking_infos_index]['revision_percentage_error_output_cost'] = output_cost
            if  selected_one_sql['fk_and_table'] == 'no':
                feedback_fk_and_table = selected_one_sql['fk_and_table_feedback']
                revised_sql, input_cost, output_cost = revision_schema_error(question, hint, revised_sql, feedback_fk_and_table, schemas_no_desc, filtered_tables_, model_name)
                all_data[linking_infos_index]['intermediate_revised_sqls'].append(revised_sql)
                all_data[linking_infos_index]['revision_schema_error_input_cost'] = input_cost
                all_data[linking_infos_index]['revision_schema_error_output_cost'] = output_cost
            if 'execution_error' in selected_one_sql.keys():
                feedback_exe_error = selected_one_sql['execution_error']
                revised_sql, input_cost, output_cost = revision_execution_error(question, hint, revised_sql, feedback_exe_error, schemas_no_desc, model_name)
                all_data[linking_infos_index]['intermediate_revised_sqls'].append(revised_sql)
                all_data[linking_infos_index]['revision_execution_error_input_cost'] = input_cost
                all_data[linking_infos_index]['revision_execution_error_output_cost'] = output_cost
            if 'predicate_no_value' in selected_one_sql.keys():
                predicate_no_values_data_add = []
                for table_values in selected_one_sql['predicate_no_value']:
                    sql_clause = "SELECT DISTINCT `{}` FROM `{}` LIMIT 3".format(table_values['column'], table_values['table'])
                    result, datas = exec_db_path_(sqlite_path, sql_clause)
                    if result != "result":
                        print(f"error: {datas}  sqlite_path:{sqlite_path}")
                        # exit(-1)
                    predicate_no_values_data_add.append(f"{table_values['table']}.{table_values['column']}:  {datas}")
                revised_sql, input_cost, output_cost = revision_db_value_error(question, hint, revised_sql, predicate_no_values_data_add, filter_similar_values, model_name)
                all_data[linking_infos_index]['intermediate_revised_sqls'].append(revised_sql)
                all_data[linking_infos_index]['revision_db_value_error_input_cost'] = input_cost
                all_data[linking_infos_index]['revision_db_value_error_output_cost'] = output_cost
            all_data[linking_infos_index]['chosed_sql_revised'] = revised_sql
        
        linking_infos_index += 1
        ques_index += 1
    with open(output_path, 'w') as out_f: 
        json.dump(all_data, out_f, indent=4)


def transform_multiple_sqls(one_sql):
    g_one_sql = Parser(one_sql)
    with_sqls = {}
    have_with_subquery = True
    with_name_len = 0
    try:
        with_name_len = len(g_one_sql.with_names)
    except Exception as e:
        have_with_subquery = False
    if with_name_len > 0:
        with_tables = []
        for i in range(with_name_len):
            with_tables.append(g_one_sql.with_names[i])
        for one_table in with_tables:
            with_sqls[one_table] = str(g_one_sql.with_queries[one_table])
        final_sub_sql = g_one_sql.query
        if "WITH" in final_sub_sql.upper():
            final_sql = re.sub(r"WITH\s+.*?\)\s*(SELECT)", r"\1", final_sub_sql, flags=re.IGNORECASE | re.DOTALL)
        else:
            final_sql = final_sub_sql
        with_sqls['final'] = final_sql
    else:
        have_with_subquery = False

    if have_with_subquery:
        multiple_sqls = ""
        for name_, sql_ in with_sqls.items():
            multiple_sqls += (name_+" : " + sql_ + "\n")
    else:
        multiple_sqls = ""
    return multiple_sqls


def verification_percentage_error(input_total_path, input_path, sl_path, output_path, model_name='deepseek-chat'):
    with open(input_path, 'r') as all_f:
        all_data = json.load(all_f)

    with open(sl_path, 'r') as sl_f: #与问题相关的schema信息  只有challenging的问题
        linking_infos = json.load(sl_f)

    with open(input_total_path, 'r') as all_total_f:
        all_total_data = json.load(all_total_f)

    all_similar_values_all = []  #这个可能没用 先保留着吧
    with open('all_similar_values_for_bird.json','r') as f_similar:  # 所有数据
        for line in f_similar.readlines():
            if line.strip():
                all_similar_values_all.append(json.loads(line)) #schemas 
    # prompt需要schema信息
    linking_infos_index = 0
    ques_index = 0
    for one_total_data in all_total_data:
        if one_total_data['difficulty'] != 'challenging':
            ques_index += 1
            continue

        if one_total_data['question'] != all_data[linking_infos_index]['question']:
            print("question are not same")
            print(f"one_total_data['question']:{one_total_data['question']}")
            print(f"all_data[linking_infos_index]['question']:{all_data[linking_infos_index]['question']}")
            exit(-1)

        db_id = all_data[linking_infos_index]['db_id']
        question = all_data[linking_infos_index]['question']
        hint = one_total_data['evidence']
        is_chess = True
        similar_values = all_similar_values_all[ques_index]
        one_linking_infos = linking_infos[linking_infos_index]
        if 'pred_all_list' not in all_data[linking_infos_index].keys():
            print('no key pred_all_list')
            exit(-1)
        # 只考虑 带 percentage的challenging 问题
        if not('percent' in question or 'percentage' in question or 'ratio' in question or 'rate' in question):
            linking_infos_index += 1
            ques_index += 1
            continue

        filter_similar_values, schemas, filtered_tables = get_filtered_schemas_with_description_new_no_desc(question, similar_values, one_linking_infos, db_id, is_chess=is_chess)
        pred_all_list = all_data[linking_infos_index]['pred_all_list']
        for one_sql_data in pred_all_list:
            one_sql = one_sql_data['one_predicted']
            #将one_sql 转成 multiple_sqls的格式
            multiple_sqls = transform_multiple_sqls(one_sql)
            verify_res, verify_exp, input_cost, output_cost = verify_percentage(question, hint, one_sql, multiple_sqls, schemas, model_name=model_name)
            one_sql_data['percentage_error'] = verify_res
            one_sql_data['percentage_error_feedback'] = verify_exp
            one_sql_data['percentage_error_input_cost'] = input_cost
            one_sql_data['percentage_error_output_cost'] = output_cost
        ques_index+=1
        linking_infos_index+=1

    
    with open(output_path, 'w') as out_f: 
        json.dump(all_data, out_f, indent=4)


'''
判断join 的主外键关系与 schema信息是否一致
'''
def verification_pk_and_table_consistency(input_total_path, input_path, sl_path, output_path, model_name='deepseek-chat'):
    with open(input_path, 'r') as all_f:
        all_data = json.load(all_f)

    with open(sl_path, 'r') as sl_f: #与问题相关的schema信息  只有challenging的问题
        linking_infos = json.load(sl_f)

    with open(input_total_path, 'r') as all_total_f:
        all_total_data = json.load(all_total_f)

    all_similar_values_all = []  #这个可能没用 先保留着吧
    with open('all_similar_values_for_bird.json','r') as f_similar:  # 所有数据
        for line in f_similar.readlines():
            if line.strip():
                all_similar_values_all.append(json.loads(line)) #schemas 
    # prompt需要schema信息
    linking_infos_index = 0
    ques_index = 0
    for one_total_data in all_total_data:
        if one_total_data['difficulty'] != 'challenging':
            ques_index+=1
            # linking_infos_index+=1
            continue
        if one_total_data['question'] != all_data[linking_infos_index]['question']:
            print("question are not same")
            print(f"one_total_data['question']:{one_total_data['question']}")
            print(f"all_data[linking_infos_index]['question']:{all_data[linking_infos_index]['question']}")
            exit(-1)

        if 'pred_all_list' not in all_data[linking_infos_index].keys():
            print('no key pred_all_list')
            exit(-1)

        db_id = all_data[linking_infos_index]['db_id']
        question = all_data[linking_infos_index]['question']
        print(f"question:{question}")
        hint = one_total_data['evidence']
        is_chess = True
        similar_values = all_similar_values_all[ques_index]
        one_linking_infos = linking_infos[linking_infos_index]
        filter_similar_values, schemas, filtered_tables = get_filtered_schemas_with_description_new_no_desc(question, similar_values, one_linking_infos, db_id, is_chess=is_chess)
        pred_all_list = all_data[linking_infos_index]['pred_all_list']
        for one_sql_data in pred_all_list:
            one_sql = one_sql_data['one_predicted']
            multiple_sqls = transform_multiple_sqls(one_sql)
            one_sql_parserd = Parser(one_sql)
            print(f"one_sql:{one_sql}")
            tables_in_sql_prompt = ""
            table_matching = True
            try:
                tables_in_sql = one_sql_parserd.tables
                if sorted(filtered_tables) == sorted(tables_in_sql):
                    tables_in_sql_prompt = f"Tables in SQL are:{tables_in_sql}, which are the same as the Tables. Thus, the tables are matching."
                else:
                    table_matching = False
                    tables_in_sql_prompt = f"Tables in SQL are:{tables_in_sql}, which are different from the Tables. Thus, there are errors as the tables are not matching. You must output 'No'."
            except Exception as e:
                print(f"error:{e}")
            verify_res, verify_exp, verify_input_cost, verify_output_cost = verify_fk_and_table(question, hint, one_sql, multiple_sqls, schemas, filtered_tables, tables_in_sql_prompt, model_name=model_name)
            if (not table_matching) and verify_res.lower() == 'yes':
                verify_res = 'no'
                verify_exp = 'Tables in SQL are different from the given Tables.'
            one_sql_data['fk_and_table'] = verify_res
            one_sql_data['fk_and_table_feedback'] = verify_exp
            one_sql_data['verify_input_cost'] = verify_input_cost
            one_sql_data['verify_output_cost'] = verify_output_cost
        ques_index+=1
        linking_infos_index+=1
    with open(output_path, 'w') as out_f: 
        json.dump(all_data, out_f, indent=4)


'''
判断执行是否出错
'''
def verification_execution_error(input_path, db_path, output_path, model_name='deepseek-chat'):
    with open(input_path, 'r') as all_f:
        all_data = json.load(all_f)

    for one_data in all_data:
        db_id = one_data['db_id']

        if 'pred_all_list' not in one_data.keys():
            print('no key pred_all_list')
            exit(-1)
        pred_all_list = one_data['pred_all_list']
        for one_sql_data in pred_all_list:
            one_sql = one_sql_data['one_predicted']
            is_error, error = sql_execute(db_id, one_sql, db_path)
            if is_error:
                one_sql_data['execution_error'] = error
    
    with open(output_path, 'w') as out_f: 
        json.dump(all_data, out_f, indent=4)



'''
根据执行结果判断是否有空值  来决定验证是否正确
'''
def verification_database_value1(input_path, db_path, output_path, model_name='deepseek-chat'):
    with open(input_path, 'r') as all_f:
        all_data = json.load(all_f)
    
    null_data_num = 0
    #all_after_decomposition_chess_output_all 是会执行每个预测的SQL的结果
    for one_data in all_data:
        db_id = one_data['db_id']
        sqlite_path = db_path + db_id +'/' + db_id +'.sqlite'
        if True:
            if 'pred_all_list' not in one_data.keys():
                print('no key pred_all_list')
                exit(-1)
            
            pred_all_list = one_data['pred_all_list']
            # for _db_path, sql_list in pred_all_list.items():
            if True:
                for one_sql_data in pred_all_list:
                    have_null_data = False
                    one_sql = one_sql_data['one_predicted']
                    hash_predicate_no_values = []
                    print(f"predicted sql:{one_sql}")
                    #如果有with 子句 要提取每个with 子句 都要执行extract_all_comparison
                    #用Parser对pred_sql进行解析
                    p_sql = Parser(one_sql)
                    parsed_sql = p_sql.query
                    comparison_list = []
                    is_have_with = True
                    try:
                        with_name_len = len(p_sql.with_names)
                    except Exception as e:
                        print(f"e:{e}\nparsed_sql:{parsed_sql}")
                        #可能这里 没有 with 子句 
                        is_have_with = False
                    with_tables = []
                    if is_have_with and with_name_len > 0 and 'WITH' in parsed_sql.upper():
                        print(f"with tables:{p_sql.with_names}")
                        for i in range(with_name_len):
                            with_tables.append(p_sql.with_names[i])
                        for one_table in with_tables:
                            sub_sql = str(p_sql.with_queries[one_table])
                            print(f"sub_sql:{sub_sql}")
                            tmp_comparison_list = extract_all_comparison(sub_sql) #提取所有predicate value
                            # print(f"tmp_comparison_list:{tmp_comparison_list}")
                            # print(f"comparison_list:{comparison_list}")
                            comparison_list.extend(tmp_comparison_list)

                        final_sql = re.sub(r"WITH\s+.*?\)\s*(SELECT)", r"\1", parsed_sql, flags=re.IGNORECASE | re.DOTALL)
                        final_comparison_list = extract_all_comparison(final_sql)
                        comparison_list.extend(final_comparison_list)

                    else:
                        tmp_comparison_list = extract_all_comparison(one_sql)
                        comparison_list.extend(tmp_comparison_list)

                    #收集完 comparison_list
                    parse_comparison_list = [extract_info_from_comparison(c) for c in comparison_list]
                    if len(comparison_list) > 0:
                        for one_comparison in parse_comparison_list:
                            if 'table_col' in one_comparison.keys() and 'val' in one_comparison.keys():
                                table_col = one_comparison['table_col']
                                extract_column = table_col[1]
                                extract_column = extract_column.lower().strip()
                                extract_val = one_comparison['right'].value

                                if extract_column not in column_to_tables[db_id]:
                                    continue
                                extract_tables =  column_to_tables[db_id][extract_column]
                                for one_extract_tb in extract_tables:
                                    # 如果列不是字符串类型 就不用比较
                                    if (column_to_uniques[db_id][extract_column][one_extract_tb][1] != "TEXT") and ("VARCHAR" not in column_to_uniques[db_id][extract_column][one_extract_tb][1]):
                                        continue 
                                    #查询数据库 判断这个database value是否存在
                                    select_query = 'SELECT `'+extract_column+'` FROM `'+one_extract_tb+'` WHERE `'+extract_column+'` = '+extract_val
                                    result, datas = exec_db_path_all(sqlite_path, select_query)
                                    if result != "result":
                                        print(f"error: {datas}  sqlite_path:{sqlite_path}")
                                        # exit(-1)
                                    else:
                                        if len(datas) == 0:
                                            if 'predicate_no_value' not in one_sql_data.keys():  #如果结果为空 证明有Database value error  这里存储的内容需要改变下
                                                one_sql_data['predicate_no_value'] = []
                                            #去掉重复的
                                            add_data = {"table":one_extract_tb, "column":extract_column, "value":extract_val}
                                            hash_add_data = generate_unique_hash(str(add_data))
                                            if hash_add_data not in hash_predicate_no_values:
                                                one_sql_data['predicate_no_value'].append(add_data)
                                                hash_predicate_no_values.append(hash_add_data)
                                            have_null_data = True
                                        else: #right
                                            pass
                            else:
                                continue
                    if have_null_data:
                        null_data_num += 1
    print(f"null_data_num:{null_data_num}")
    with open(output_path, 'w') as out_f: 
        json.dump(all_data, out_f, indent=4)



'''
判断返回列 是否与问题 匹配
'''
def verification_returned_column(input_path):
    with open(input_path, 'r') as all_f:
        all_data = json.load(all_f)

    for one_data in all_data:
        pass



if __name__ == '__main__':
    pass