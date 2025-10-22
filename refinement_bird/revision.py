import json
import os
import sqlparse
from utils.utils import get_tables 
import copy

import random 
from revision_10_30_util import *

# from from_spider_whole_codes.transform_style_for_generate_sql import *


databases = dict()
datebase_content = dict()


def _get_tables(db_id):
    if db_id in databases:
        return databases[db_id]
    else:
        path_db = os.path.join('../data/BIRD/databases/', db_id, db_id + ".sqlite")
        tables = get_tables(path_db)
        databases[db_id] = tables
        return tables


def get_filtered_schemas_with_description_new_no_desc(original_question, similar_values, linking_infos, db_id, is_chess):
    #得到当前问题的schema 信息
    if original_question != linking_infos['original_question']:
        print(f"question is not the same \n original_question:{original_question}  \n linking_infos:{linking_infos['original_question']}")
        exit(-1)

    db_path = os.path.join('../data/BIRD/databases/', db_id, db_id + ".sqlite")
    cursor = get_cursor_from_path(db_path)
    
    filter_similar_values = list()
    #开始提取 schema 信息
    schemas = ""
    table_to_columns = dict()
    tables  = _get_tables(db_id)
    column_types = linking_infos["column_types"] #column_names数据库中所有表的type  与 column_names格式相同
    table_names = linking_infos["table_names"]

    if is_chess:
        table_labels = linking_infos["selected_tables"]  #TODO table label和 column label 记得改下
        column_labels = linking_infos["selected_columns"] #预测的结果
    else:
        table_labels = linking_infos["table_labels"] 
        column_labels = linking_infos["column_labels"]
    

    column_names = linking_infos["column_names"]
    column_description = linking_infos["column_description"]
    cur_col_index = 0
    column_list = []
    table_list = []
    filtered_tables = []
    for tlabel_id in range(len(table_labels)):
        if table_labels[tlabel_id] == 1:
            table_list.append(table_names[tlabel_id])
            table_to_columns[table_names[tlabel_id]] = []
            for clabel_in in range(len(column_labels[tlabel_id])):
                if column_labels[tlabel_id][clabel_in] == 1: #加上对应的column name 和 column comment 用cur_col_index 表示 因为这两个list都是一维的
                    table_to_columns[table_names[tlabel_id]].append([column_names[cur_col_index], column_description[cur_col_index]["column_description"],column_description[cur_col_index]["value_description"], column_types[cur_col_index]])
                    column_list.append(column_names[cur_col_index])
                    column_list.append(column_description[cur_col_index])
                cur_col_index += 1
        else:
            cur_col_index += len(column_labels[tlabel_id])

    cul_column_labels = [0]
    for i in range(len(table_labels)):
        cul_column_labels.append(cul_column_labels[i-1]+len(column_labels[i]))
    for table, columns in table_to_columns.items():
        schemas += f"CREATE TABLE `{table}`\n(\n"
        filtered_tables.append(table)
        #column 可以加入 description 信息
        #先把所有primary key加进去
        for one_table in tables:
            if one_table['name'] == table.lower().strip():
                # if one_table['name'] == 'atom':
                #     print(one_table['table_info']['primary_key'])
                for ii in range(len(table_labels)):
                    if table_names[ii].lower().strip() == table.lower().strip():
                        # print('test1111')
                        for jj in range(cul_column_labels[ii], cul_column_labels[ii+1]+1):
                            if column_names[jj].lower().strip() in one_table['table_info']['primary_key']:
                                schemas += f"\t'{column_names[jj]}'  {column_types[jj].upper()} PRIMARY KEY,\n"
        for one_column in columns:
            # schemas += f"\t'{one_column[0]}'  {one_column[3].upper()}  --description: {one_column[1]}. value description: {one_column[2]}.\n"
            # 不加入 description 信息
            # 加上primary key
            for one_table in tables:
                if one_table['name'] == table.lower().strip():
                    if one_column[0].lower().strip() not in one_table['table_info']['primary_key']:
                        schemas += f"\t'{one_column[0]}'  {one_column[3].upper()},\n"
                    break
        
        if table.lower().strip() in similar_values.keys():
            for similar_one_column in similar_values[table.lower().strip()].keys():
                filter_similar_values.append(f"{table}.{similar_one_column} : {similar_values[table.lower().strip()][similar_one_column.lower().strip()]}")
        
        foreignKeySet = []
        for one_table in tables: #每个table
            # print(one_table["table_info"])
            if one_table['name'] != table.lower().strip():
                continue
            for pair_str in one_table["table_info"]["foreign_key"]:
                a, b = [_.strip() for _ in pair_str[1:-1].split(",")]
                a_list = a.split('.')
                b_list = b.split('.')
                if b_list[0].lower().strip() != table.lower().strip() and a_list[0].lower().strip() == table.lower().strip() and a_list[0].lower().strip() in table_list and b_list[0].lower().strip() in table_list: 
                    oneForeignKey = f"FOREIGN KEY '{a_list[1]}' REFERENCES `{b_list[0]}`('{b_list[1]}'),\n"
                    if oneForeignKey not in foreignKeySet: 
                        schemas += oneForeignKey
                        foreignKeySet.append(oneForeignKey)
        schemas += ");\n"

    return filter_similar_values, schemas, filtered_tables


def get_filtered_schemas_with_description_new(original_question, similar_values, linking_infos, db_id, is_chess):
    #得到当前问题的schema 信息
    if original_question != linking_infos['original_question']:
        print(f"question is not the same \n original_question:{original_question}  \n linking_infos:{linking_infos['original_question']}")
        exit(-1)

    db_path = os.path.join('../data/BIRD/databases/', db_id, db_id + ".sqlite")
    cursor = get_cursor_from_path(db_path)
    
    filter_similar_values = list()
    #开始提取 schema 信息
    schemas = ""
    table_to_columns = dict()
    tables  = _get_tables(db_id)
    column_types = linking_infos["column_types"] #column_names数据库中所有表的type  与 column_names格式相同
    table_names = linking_infos["table_names"]

    if is_chess:
        table_labels = linking_infos["selected_tables"]  
        column_labels = linking_infos["selected_columns"] #预测的结果
    else:
        table_labels = linking_infos["table_labels"] 
        column_labels = linking_infos["column_labels"]
    
    column_names = linking_infos["column_names"]
    column_description = linking_infos["column_description"]
    cur_col_index = 0
    column_list = []
    table_list = []
    filtered_tables = []
    for tlabel_id in range(len(table_labels)):
        if table_labels[tlabel_id] == 1:
            table_list.append(table_names[tlabel_id])
            table_to_columns[table_names[tlabel_id]] = []
            for clabel_in in range(len(column_labels[tlabel_id])):
                if column_labels[tlabel_id][clabel_in] == 1: #加上对应的column name 和 column comment 用cur_col_index 表示 因为这两个list都是一维的
                    table_to_columns[table_names[tlabel_id]].append([column_names[cur_col_index], column_description[cur_col_index]["column_description"],column_description[cur_col_index]["value_description"], column_types[cur_col_index]])
                    column_list.append(column_names[cur_col_index])
                    column_list.append(column_description[cur_col_index])
                cur_col_index += 1
        else:
            cur_col_index += len(column_labels[tlabel_id])
    for table, columns in table_to_columns.items():
        schemas += f"CREATE TABLE `{table}`\n(\n"
        filtered_tables.append(table)
        #column 可以加入 description 信息
        for one_column in columns:
            schemas += f"\t'{one_column[0]}'  {one_column[3].upper()}  --description: {one_column[1]}. value description: {one_column[2]}.\n"
        
        if table.lower().strip() in similar_values.keys():
            for similar_one_column in similar_values[table.lower().strip()].keys():
                filter_similar_values.append(f"{table}.{similar_one_column} : {similar_values[table.lower().strip()][similar_one_column.lower().strip()]}")
        
        foreignKeySet = []
        for one_table in tables: #每个table
            for pair_str in one_table["table_info"]["foreign_key"]:
                a, b = [_.strip() for _ in pair_str[1:-1].split(",")]
                #判断 如果两个 column  a和b都在出现的schema中
                a_list = a.split('.')
                b_list = b.split('.')
                if b_list[0].lower().strip() != table.lower().strip() and a_list[0].lower().strip() in table_list and b_list[0].lower().strip() in table_list:  #只需要判断两个table 相似即可  
                    oneForeignKey = f"FOREIGN KEY '{a_list[1]}' REFERENCES `{b_list[0]}`('{b_list[1]}')\n"
                    if oneForeignKey not in foreignKeySet: #加一个判断 避免出现重复的主外键关系
                        schemas += oneForeignKey
                        foreignKeySet.append(oneForeignKey)
        schemas += ");\n"
    return filter_similar_values, schemas, filtered_tables




def get_filtered_schemas_with_description(original_question, similar_values, linking_infos, db_id, is_chess):
    #得到当前问题的schema 信息
    if original_question != linking_infos['original_question']:
        print(f"question is not the same \n original_question:{original_question}  \n linking_infos:{linking_infos['original_question']}")
        exit(-1)

    db_path = os.path.join('../data/BIRD/databases/', db_id, db_id + ".sqlite")
    cursor = get_cursor_from_path(db_path)
    
    filter_similar_values = list()
    #开始提取 schema 信息
    schemas = ""
    table_to_columns = dict()
    date_dicts = dict() #记录date datetime  
    date_time_dicts = dict() #date日期数据 但是是TEXT类型  记录每个属性的data type
    time_dicts = dict()
    tables  = _get_tables(db_id)
    column_types = linking_infos["column_types"] #column_names数据库中所有表的type  与 column_names格式相同
    table_names = linking_infos["table_names"]

    if is_chess:
        table_labels = linking_infos["selected_tables"]  #TODO table label和 column label 记得改下
        column_labels = linking_infos["selected_columns"] #预测的结果
    else:
        table_labels = linking_infos["table_labels"] 
        column_labels = linking_infos["column_labels"]
    

    column_names = linking_infos["column_names"]
    column_description = linking_infos["column_description"]
    cur_col_index = 0
    column_list = []
    table_list = []
    filtered_tables = []
    for tlabel_id in range(len(table_labels)):
        if table_labels[tlabel_id] == 1:
            table_list.append(table_names[tlabel_id])
            table_to_columns[table_names[tlabel_id]] = []
            for clabel_in in range(len(column_labels[tlabel_id])):
                if column_labels[tlabel_id][clabel_in] == 1: #加上对应的column name 和 column comment 用cur_col_index 表示 因为这两个list都是一维的
                    table_to_columns[table_names[tlabel_id]].append([column_names[cur_col_index], column_description[cur_col_index]["column_description"],column_description[cur_col_index]["value_description"], column_types[cur_col_index]])
                    column_list.append(column_names[cur_col_index])
                    column_list.append(column_description[cur_col_index])
                cur_col_index += 1
        else:
            cur_col_index += len(column_labels[tlabel_id])
    for table, columns in table_to_columns.items():
        schemas += f"CREATE TABLE `{table}`\n(\n"
        filtered_tables.append(table)
        #column 可以加入 description 信息
        for one_column in columns:
            samples = None
            #下面三种情况都要加更新database_content所以全放到一起就行
            # 可能列名并不包含 time 但是在description中包含time
            if 'date' in one_column[3].lower().strip() or 'date' in one_column[0].lower().strip() or 'birthday' in one_column[0].lower().strip() or 'time' in one_column[0].lower().strip() or 'time' in one_column[1].lower().strip():
                sql_clause = "SELECT DISTINCT `{}` FROM `{}` LIMIT 3".format(one_column[0].lower().strip(), table)
                # print(f"sql_caluse:{sql_clause}")
                if table.lower().strip() in datebase_content.keys():
                    if one_column[0].lower().strip() in datebase_content[table.lower().strip()].keys():
                        samples = datebase_content[table.lower().strip()][one_column[0].lower().strip()]
                    else:
                        results = execute_sql(cursor, sql_clause)
                        samples = [result[0] for result in results]
                        datebase_content[table.lower().strip()][one_column[0].lower().strip()] = samples
                else:
                    results = execute_sql(cursor, sql_clause)
                    samples = [result[0] for result in results]
                    datebase_content[table.lower().strip()] = dict()
                    datebase_content[table.lower().strip()][one_column[0].lower().strip()] = samples

            if 'date' in one_column[3].lower().strip(): #data type 为DATE 或者DATETIME类型时
                #从这一列中select几个数
                if table.lower().strip() not in date_dicts.keys():
                    date_dicts[table.lower().strip()] = dict()
                date_dicts[table.lower().strip()][one_column[0].lower().strip()]=[one_column[3].lower().strip(), samples]
            elif 'date' in one_column[0].lower().strip() or 'birthday' in one_column[0].lower().strip(): #列名带有date/birthday字样  但是data type 是TEXT
                if table.lower().strip() not in date_time_dicts.keys():
                    date_time_dicts[table.lower().strip()] = dict()
                date_time_dicts[table.lower().strip()][one_column[0].lower().strip()]=[one_column[3].lower().strip(), samples]
            elif 'time' in one_column[0].lower().strip() or 'time' in one_column[1].lower().strip():
                if table.lower().strip() not in time_dicts.keys():
                    time_dicts[table.lower().strip()] = dict()
                time_dicts[table.lower().strip()][one_column[0].lower().strip()]=[one_column[3].lower().strip(), samples]
            
            schemas += f"\t'{one_column[0]}'  {one_column[3].upper()}  --description: {one_column[1]}. value description: {one_column[2]}.\n"
        
        if table.lower().strip() in similar_values.keys():
            for similar_one_column in similar_values[table.lower().strip()].keys():
                filter_similar_values.append(f"{table}.{similar_one_column} : {similar_values[table.lower().strip()][similar_one_column.lower().strip()]}")
        
        foreignKeySet = []
        for one_table in tables: #每个table
            for pair_str in one_table["table_info"]["foreign_key"]:
                a, b = [_.strip() for _ in pair_str[1:-1].split(",")]
                #判断 如果两个 column  a和b都在出现的schema中
                a_list = a.split('.')
                b_list = b.split('.')
                if b_list[0].lower().strip() != table.lower().strip() and a_list[0].lower().strip() in table_list and b_list[0].lower().strip() in table_list:  #只需要判断两个table 相似即可  
                    oneForeignKey = f"FOREIGN KEY '{a_list[1]}' REFERENCES `{b_list[0]}`('{b_list[1]}')\n"
                    if oneForeignKey not in foreignKeySet: #加一个判断 避免出现重复的主外键关系
                        schemas += oneForeignKey
                        foreignKeySet.append(oneForeignKey)
        schemas += ");\n"
    return filter_similar_values, schemas, date_dicts, date_time_dicts, time_dicts, filtered_tables


def revision_by_database_content(schemas, filter_similar_values, question, evidence, pred_sql):
    prompt = '''You are a good SQL expert, you need to revise incorrect the SQL based on the user question.
Nest, I will provide the filtered database schema, and database content of each column relevant to the question. 

Filtered Database Schema:
{}


Given the following question (with evidence), and the predicted SQL. Please revise the predicted SQL, or output "Correct" if you think it is right.
Please directly output the revised SQL or "Correct" without explanation. 
The SQL cannot contradict evidence. {}

Question: {}
Evidence: {}
{}
Predicted SQL: {}
Revision:
'''
    filter_similar_values_prompt = ""
    database_content_prompt = ""
    if len(filter_similar_values)>0:
        filter_similar_values_prompt+="\nPlease check the similar database content of corresponding table.column to correct the predicate value. \n1. If the content in the following matches the predicate value but differs only in letter case ,or they are slightly different, update it to match the content in the database. \n2. If the content is completely different from the predicate value but there is a clear reference in the question, then you do not need to match the database content.\n\n"
        database_content_prompt = "Similar Database content:"
        for one_similar_values in filter_similar_values:
            database_content_prompt+=one_similar_values+"\n"
    prompt = prompt.format(schemas, filter_similar_values_prompt, question, evidence, database_content_prompt, pred_sql)
    print(f"prompt revision_by_database_content :\n{prompt}")
    

    return call_gpt(prompt)


'''
下面的规则不用大模型就可以
6. When the SQL needs join multiple tables, you must use INNER JOIN.

7. It is recommend to use `` to enclose column name and table name.
'''
def revision_by_instructions_and_time(question, evidence, pred_sql, date_dicts, date_time_dicts, time_dicts):
    prompt = '''You are a good SQL expert, you need to revise incorrect the SQL based on the user question.
Nest, I will provide some instructions. 

Instructions:
1. When expressing a range for a column, it's best to use the keyword BETWEEN, such as: column_name BETWEEN a AND b.
2. You should use IN operator between predicate column and sub-SQL, such as WHERE column IN sub-SQL.
3. Each return column should determine whether it is NOT NULL, such as SELECT column FROM table WHERE column IS NOT NULL.
4. Each order by column should determine whether it is NOT NULL, such as SELECT * FROM table WHERE column IS NOT NULL ORDERBY column.
5. When there are multiple table joins in SQL, each column name in the SQL must be concatenated with its corresponding table name. For example, table.column.
6. Rate does not need to be multiplied by 100, but percent does.


{}

Given the following question (with evidence), and the predicted SQL. Please revise the predicted SQL, or output "Correct" if you think it is right.
Please directly output the revised SQL or "Correct" without explanation. Note that the evidence is also important.
The SQL cannot contradict evidence.

Question: {}
Evidence: {}
Predicted SQL: {}
Revision:
'''
    addtional_instructions =""
    if len(date_time_dicts)>0 or len(date_dicts)>0 or len(time_dicts)>0:
        addtional_instructions+="Additional Instructions:\n"
        if len(date_dicts)>0:
            addtional_instructions+="The following column describe information about date. Their data type is DATE or DATETIME, you must use STRFTIME to extract year, month, etc. For example, STRFTIME(%Y, date) can extract the year.\n\n"
            for table, columns in date_dicts.items():
                for column, content in columns.items():
                    addtional_instructions+=f"{table}.{column} {content[0]}  content: {content[1]}\n"
            addtional_instructions +="\n\n"
        if len(date_time_dicts)>0:
            addtional_instructions+="The following column describe information about date. However, their data type is TEXT, so you must use SUBSTR / SUBSTRING to extract year (SUBSTR(1,4)), month (SUBSTR(5, 2)), etc. You need to determine the extraction position based on the specific data content.\n\n"
            for table, columns in date_time_dicts.items():
                for column, content in columns.items():
                    addtional_instructions+=f"{table}.{column} {content[0]}  content: {content[1]}\n"
            addtional_instructions +="\n\n"
        if len(time_dicts)>0:
            addtional_instructions+="The following column describe information about time. Their data type is TEXT, so you must use SUBSTR / SUBSTRING to extract hour, minutes, second, etc. \nAdditionally, If the question need to search minutes, but the content has second, we can use LIKE time% to search. For example we need to search 0:01:23, but the time format of database content is 1:26.714, we use LIKE 1:23% to search. \nYou need to determine the extraction position based on the specific data content.\n\n"
            for table, columns in time_dicts.items():
                for column, content in columns.items():
                    addtional_instructions+=f"{table}.{column} {content[0]}  content: {content[1]}\n"
            addtional_instructions +="\n\n"
    prompt = prompt.format(addtional_instructions, question, evidence, pred_sql)
    print(f"prompt revision_by_instructions_and_time :\n{prompt}")
    
    return call_gpt(prompt)
    
def revision_select_column(question, evidence, pred_sql):
    prompt = '''You are good SQL writer, I will provide you a question, the corresponding predicted SQL, you need to determine the return column (i.e., the SELECT columns in the SQL) is right or not. 
There are two types of errors in return column:

1. Return redundant columns in the SQLs, compared to the question. Please donot revise the current column, you can only discard some redundant column. Note that `id` can indicates `name`. Given the following examples:

Example 1:
Question : Name movie titles released in year 1945. Sort the listing by the descending order of movie popularity.
Evidence : released in the year 1945 refers to movie_release_year = 1945. 
Predicted SQL : SELECT movie_title, movie_type FROM movies WHERE movie_release_year = 1945 ORDER BY movie_popularity DESC LIMIT 1
Chain of Thought : The question only needs to output the movie title, so we cannot output the movie type additionally.
Revision : SELECT movie_title FROM movies WHERE movie_release_year = 1945 ORDER BY movie_popularity DESC LIMIT 1

Example 2:
Question : How many movies directed by Francis Ford Coppola have a popularity of more than 1,000? Indicate what is the highest amount of likes that each critic per movie has received, if there's any.
Evidence : Francis Ford Coppola refers to director_name; popularity of more than 1,000 refers to movie_popularity >1000;highest amount of likes that each critic per movie has received refers to MAX(critic_likes).
Predicted SQL : SELECT COUNT(T2.movie_title), director, T1.critic FROM ratings AS T1 INNER JOIN movies AS T2 ON T1.movie_id = T2.movie_id WHERE T2.director_name = 'Francis Ford Coppola' AND T2.movie_popularity > 1000
Chain of Thought: The question needs to return how many movies (COUNT(T2.movie_title)) and what is the highest amount of likes (MAX(critic_likes)). So the director cannot be returned.
Revision : SELECT COUNT(T2.movie_title), T1.critic FROM ratings AS T1 INNER JOIN movies AS T2 ON T1.movie_id = T2.movie_id WHERE T2.director_name = 'Francis Ford Coppola' AND T2.movie_popularity > 1000


2. Incorrectly merge returned columns. This error usually occurs when needing to return the full name. Given the following example:

Example:
Question: Find the full name of the player born in Atlanta and have the highest number of blocks. Also, in which team did this player perform the most number of blocks?
Evidence: full name refers to first_name, middle_name, last_name; born in Atlanta refers to birthCity = 'Atlanta'; the highest number of blocks refers to max(blocks); team refers to tmID
Predicted SQL: SELECT T1.firstName || ' ' || T1.lastName, T2.tmID FROM players AS T1 INNER JOIN players_teams AS T2 ON T1.playerID = T2.playerID WHERE T1.birthCity = 'Atlanta' ORDER BY T2.blocks DESC LIMIT 1
Chain of Thought: We cannot merge the firstname (forename) and lastname (surname) into one column, which needs to be output separately.
Revision: SELECT T1.firstName, T1.lastName, T2.tmID FROM players AS T1 INNER JOIN players_teams AS T2 ON T1.playerID = T2.playerID WHERE T1.birthCity = 'Atlanta' ORDER BY T2.blocks DESC LIMIT 1

Given the following question and the predicted SQL, Please directly output the revision SQL without explanation.
If you think there are no errors in the SQL, please directly output "Correct":
Note that you can only revise the SELECT column in SQL, you cannot modify other parts of the SQL statement.

Question: {}
Evidence: {}
Predicted SQL: {}
Revision: 
'''
    
    prompt = prompt.format(question, evidence, pred_sql)
    print(f"prompt revision_select_column :{prompt}")

    return call_gpt(prompt)



def revision_execution_error(question, evidence, pred_sql, exec_error, schemas):
    prompt = '''## You are an NL2SQL expert. For the given SQL statement, I need you to use the relevant information to correct it. 
The information provided includes the SQL statement, its corresponding natural language, execution error messages, and the database schema information. 
## You need to check whether the table name, column name, and joins of the table are correct according to execution error.

Please output the modified SQL statement directly, without any explanation.

Question: {}
Evidence: {}
Predicted SQL: {}

Execution Error: {}

Schemas:
{}
'''
    prompt = prompt.format(question, evidence, pred_sql, exec_error, schemas)

    return call_gpt(prompt)





def revision(all_path, sl_path, output_revision_path, is_all = False, is_chess = True):
    '''
    sl_path :test_schema-linking.jsonl
    '''
    #所有数据
    with open(all_path, 'r') as all_f:
        all_data = json.load(all_f)

    #加载 schema linking文件   只有challenging 数据
    linking_infos = []
    with open(sl_path, 'r') as sl_f:
        linking_infos = json.load(sl_f)


    print(f"len of all_data:{len(all_data)}")
    print(f"len of linking_infos:{len(linking_infos)}")

    ques_index = 0

    all_similar_values_all = []
    with open('all_similar_values_for_bird.json','r') as f_similar:  # 所有数据
        for line in f_similar.readlines():
            if line.strip():
                all_similar_values_all.append(json.loads(line)) #schemas 

    ques_index = 0
    linking_infos_index = 0
    for one_data in all_data:

        difficulty = one_data['difficulty']
        question = one_data['question']
        evidence = one_data['evidence']
        db_id = one_data['db_id']
        
        if difficulty == 'challenging':
            if (not is_all) and one_data['exec_score']['res'] == 1: #只对错误的进行revision
                ques_index+=1
                linking_infos_index+=1
                continue
            print(f"question:{question}")
            pred_sqls = one_data['pred'] #待修改sql
            similar_values = all_similar_values_all[ques_index]
            one_linking_infos = linking_infos[linking_infos_index]
        
            #question  不包含 evidence
            filter_similar_values, schemas, date_dicts, date_time_dicts, time_dicts, filtered_tables = get_filtered_schemas_with_description(question, similar_values, one_linking_infos, db_id, is_chess=is_chess)
            print(f"filter_similar_values:{filter_similar_values}")

        
            revised_sql_by_database_contents = []
            revised_sql_by_instructions_and_times = []
            sql_after_revision_select_columns = []
            sql_after_revision_executions = []
            predicted_sqls = pred_sqls.split(";")
            for one_predicted in predicted_sqls:
                if one_predicted != "" and "select" in one_predicted.lower().strip():
                    print(f"one_predicted:{one_predicted}")
                    prompt_revision_by_database_content, revised_sql_by_database_content = revision_by_database_content(schemas, filter_similar_values, question, evidence, one_predicted)
                    print(f"prompt_revision_by_database_content:{prompt_revision_by_database_content}")
                    print(f"revised_sql_by_database_content:{revised_sql_by_database_content}")

                    if "correct" in revised_sql_by_database_content.lower().strip():
                        revised_sql_by_database_content = copy.deepcopy(one_predicted)
                    revised_sql_by_database_contents.append(copy.deepcopy(revised_sql_by_database_content))
                    print(f"after revised_sql_by_database_content:{revised_sql_by_database_content}")

                    prompt_revision_by_instructions_and_time, revised_sql_by_instructions_and_time = revision_by_instructions_and_time(question, evidence, revised_sql_by_database_content, date_dicts, date_time_dicts, time_dicts)
                    print(f"prompt_revision_by_instructions_and_time:{prompt_revision_by_instructions_and_time}")
                    print(f"revised_sql_by_instructions_and_time:{revised_sql_by_instructions_and_time}")

                    if "correct" in revised_sql_by_instructions_and_time.lower().strip():
                        revised_sql_by_instructions_and_time = copy.deepcopy(revised_sql_by_database_content)
                    revised_sql_by_instructions_and_times.append(copy.deepcopy(revised_sql_by_instructions_and_time))
                    print(f"after revised_sql_by_instructions_and_time:{revised_sql_by_instructions_and_time}")

                    prompt_revision_select_column, sql_after_revision_select_column = revision_select_column(question, evidence, revised_sql_by_instructions_and_time)
                    print(f"prompt_revision_select_column:{prompt_revision_select_column}")
                    print(f"sql_after_revision_select_column:{sql_after_revision_select_column}")

                    if 'correct' in sql_after_revision_select_column.lower().strip(): #select column 没有错误
                        sql_after_revision_select_column = copy.deepcopy(revised_sql_by_instructions_and_time)
                    sql_after_revision_select_columns.append(copy.deepcopy(sql_after_revision_select_column))
                    print(f"after sql_after_revision_select_column:{sql_after_revision_select_column}")
        
                    sql_after_revision_select_column = sql_after_revision_select_column.strip('"')
                    sql_after_revision_select_column = sql_after_revision_select_column.strip("\n")
                    sql_after_revision_select_column = sql_after_revision_select_column.strip('"')
                    sql_after_revision_select_column = sql_after_revision_select_column.strip('\\')
                    sql_after_revision_select_column = sql_after_revision_select_column.replace("\n"," ")
                    sql_after_revision_select_column = sql_after_revision_select_column.replace("Revised SQL:","")
                    sql_after_revision_select_column = sql_after_revision_select_column.replace("Revision:","")
                    sql_after_revision_select_column = sql_after_revision_select_column.replace("```"," ")
                    sql_after_revision_select_column = sql_after_revision_select_column.replace("|| ' ' ||",",")
                    sql_after_revision_select_column = sql_after_revision_select_column.replace("sql"," ")
                    sql_after_revision_select_column = sql_after_revision_select_column.replace("\\"," ")
                    sql_after_revision_select_column = sql_after_revision_select_column.strip()

                    sql_after_revision_select_column_split = sql_after_revision_select_column.split(";")
                    for one_sql in sql_after_revision_select_column_split:
                        if "select" in one_sql.lower().strip():
                            sql_after_revision_select_column = one_sql
                            break

                    db_path = os.path.join('../data/BIRD/databases/', db_id, db_id + ".sqlite")
                    exec_error = execute_sql_for_error(sql_after_revision_select_column, db_path) #看是否执行出错

                    exec_error = str(exec_error)
                    if "correct" not in  exec_error.lower().strip():
                        prompt_after_revision_execution, sql_after_revision_execution = revision_execution_error(question, evidence, sql_after_revision_select_column, exec_error, schemas)
                        print(f"prompt_after_revision_execution:{prompt_after_revision_execution}")
                        print(f"sql_after_revision_execution:{sql_after_revision_execution}")
                    else:
                        sql_after_revision_execution = sql_after_revision_select_column
                    sql_after_revision_executions.append(copy.deepcopy(sql_after_revision_execution))
                    print(f"after sql_after_revision_execution:{sql_after_revision_execution}")

            one_data['revised_sql_by_database_contents'] = revised_sql_by_database_contents
            one_data['revised_sql_by_instructions_and_times'] = revised_sql_by_instructions_and_times
            one_data['sql_after_revision_select_columns'] = sql_after_revision_select_columns
            one_data['sql_after_revision_executions'] = sql_after_revision_executions
            linking_infos_index += 1
        ques_index += 1
    
    with open(output_revision_path, 'w') as outpu_f:
        json.dump(all_data, outpu_f, indent=4)


if __name__ == '__main__':
    pass