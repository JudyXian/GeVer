import dashscope
import anthropic
import openai
from http import HTTPStatus
import sqlite3
import random
import os
import time
import numpy as np
import json


MODEL35='gpt-3.5-turbo'
OPENAIKEY=''
BASE_URL = 'https://api.openai.com/v1'

DEEPSEEKKEY=''
DEEPSEEK_BASE_URL='https://api.deepseek.com'


def ask_llm(_model, prompt, _base_url, _api_key):
    client = openai.OpenAI(api_key=_api_key, base_url=_base_url)

    response = client.chat.completions.create(
        model=_model,
        messages=[
            {"role": "user", "content": prompt},
        ],
        stream=False
    )
    return response.choices[0].message.content, response.usage.prompt_tokens, response.usage.completion_tokens

def call_gpt(prompt, model_name):
    max_cnt = 15
    cur_cnt = 0
    input_cost = 0
    output_cost = 0
    while cur_cnt < max_cnt:
        try:
            if 'deepseek' in model_name:
                res, input_cost, output_cost = ask_llm(model_name, prompt, DEEPSEEK_BASE_URL, DEEPSEEKKEY)
            else:
                res, input_cost, output_cost = ask_llm(model_name, prompt, BASE_URL, OPENAIKEY)
            return prompt, res, input_cost, output_cost
        except Exception as e:
            cur_cnt+=1
            print(e)
            continue
    return None, None, input_cost, output_cost


def call_qwen(prompt):
    max_cnt = 15
    cur_cnt = 0
    while cur_cnt < max_cnt:
        try:
            messages = [{'role': 'system', 'content': 'You are a helpful assistant.'},
                {'role': 'user', 'content': prompt}]
            dashscope.api_key = ''
            response = dashscope.Generation.call(model="qwen-long",
                               messages=messages,
                               # 设置随机数种子seed，如果没有设置，则随机数种子默认为1234
                               seed=random.randint(1, 10000),
                               # 将输出设置为"message"格式
                               result_format='message')
            ans = ''
            if response.status_code == HTTPStatus.OK:
                ans = response.output.choices[0].message.content
            else:
                print('Request id: %s, Status code: %s, error code: %s, error message: %s' % (
                    response.request_id, response.status_code,
                    response.code, response.message
                ))
        
            return prompt, ans
        except Exception as e:
            cur_cnt+=1
            print(e)
            continue
    return None, None

def call_claude(prompt):
    max_cnt = 15
    cur_cnt = 0
    while cur_cnt < max_cnt:
        try:
            client_claude = anthropic.Anthropic(
                api_key="",
            )
            message = client_claude.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=4096,
                messages=[
                {"role": "user", "content": prompt}
                ]
            )
            ans = message.content[0].text
        
            return prompt, ans
        except Exception as e:
            cur_cnt+=1
            print(e)
            continue
    return None, None



def precision_recall_f1(predicted, actual):
    # 计算 True Positive, False Positive, True Negative, False Negative
    tp = sum(int(p == 1 and p == a) for p, a in zip(predicted, actual))
    fp = sum(int(p == 1 and p != a) for p, a in zip(predicted, actual))
    tn = sum(int(p == 0 and p == a) for p, a in zip(predicted, actual))
    fn = sum(int(p == 0 and p != a) for p, a in zip(predicted, actual))
 
    # 计算精度 Precision
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
 
    # 计算召回率 Recall
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
 
    # 计算 F1 分数
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
 
    return precision, recall, f1, tp




def gpt_new(prompt, _model, _api_key, _base_url):
    client = openai.OpenAI(api_key=_api_key, base_url=_base_url)

    response = client.chat.completions.create(
        model=_model,
        messages=[
            # {"role": "system", "content": "You are a helpful assistant"},
            {"role": "user", "content": prompt},
        ],
        stream=False
    )
    # answer = response['choices'][0]['message']['content']
    answer = response.choices[0].message.content
    consistency = answer.split('Explanation:')[0]


    if 'yes' in consistency.lower():
        consistency = 'yes'
    else:
        consistency = 'no'
    explanation = answer.split('Explanation:')[1].strip()
        
    print(f"consistency:\n{consistency}")
    print(f"explanation:\n{explanation}")
    return consistency, explanation, response.usage.prompt_tokens, response.usage.completion_tokens



def prompt_verify(question, hint, pred_sql, sql_explanations, schemas, model_name = 'deepseek-chat'):
    prompt_verification = '''
#### You are a SQL expert. Please verify if the SQL query correctly corresponds to the NL Question with the corresponding hint. The natural language description of the SQL (NL Description) can help you better understand the SQL query and its intent.

## NL Question
{}
## Hint
{}

## SQL Query:
{}

## NL Description:
{}

## Schema Information:
{}

## Given some notes that help you do determination:
1. You first determine Whether the columns returned by the SQL Query fully match those in the NL Question. If the SQL query consists of multiple sub-queries formed with "WITH", focus on whether the content returned by the final sub-query matches the NL Question requirements.
The order of returned columns in the SQL can differ from the order in the question.
2. If you think that certain information in the SQL is redundant but does not affect the semantics or result, such SQL is considered correct and output 'yes'.
3. Two conditions connected by the "OR" operator should be enclosed in parentheses, such as WHERE (date > 1991 OR date < 1980).
4. Aliases can be enclosed in quotes, such as 'sub1', 'sub2', etc. 
5. The evidence is also important, and the semantics of the SQL must fully align with the meaning of the evidence.
6. In the database "toxicology", all elements are considered as toxic.

### Output Format:
Answer: Yes or No
Explanation: Please explain your reasoning.
'''
    prompt_verification = prompt_verification.format(question, hint, pred_sql, sql_explanations, schemas)
    print(f"prompt_verification:\n{prompt_verification}")

    n_repeat = 0
    max_cnt = 15
    consistency = ''
    explanation = ''
    while n_repeat < max_cnt:
        try:
            if 'gpt' in model_name:
                consistency, explanation, input_cost, output_cost = gpt_new(prompt_verification, model_name, OPENAIKEY, BASE_URL)
            elif 'deepseek' in model_name:
                consistency, explanation, input_cost, output_cost = gpt_new(prompt_verification, model_name, DEEPSEEKKEY, DEEPSEEK_BASE_URL)
            else:
                print('model_name:'+str(model_name)+'   error')
                exit(-1)
            break
        except Exception as e:
            n_repeat += 1
            print(
                f"Repeat for the {n_repeat} times for exception: {e}", end="\n")
            time.sleep(1)
            continue
    return consistency, explanation, input_cost, output_cost



def prompt_verify_no_nl(question, hint, pred_sql, exec_result, schemas, filter_tables, filter_similar_values):
    pass


def generate_nl_by_llm(sub_sql, sub_nl, alias_maps_tb):
    generate_nl_by_llm_prompt = '''
You are a SQL expert. 

Given the following SQL query, you need to translate it into the corresponding natural language without neglecting semantic details.
Note the following SQL snippets are not complete SQL statements. They start with parts like SELECT, WHERE, GROUP BY, ORDER BY, or FROM. 
In addition to the SQL snippets, we provide natural language descriptions generated according to certain rules (which may not be entirely accurate), and the mappings for table aliases (If have).

SQL:
{}

NL by rules:
{}

{}

Please directly output the natural language without explanation.
'''
    alias_maps_string = ""
    if len(alias_maps_tb) > 0:
        alias_maps_string += "Alias mapping:\n"
        alias_maps_string += str(alias_maps_tb)
    generate_nl_by_llm_prompt = generate_nl_by_llm_prompt.format(sub_sql, sub_nl, alias_maps_string)
    print(f"generate_nl_by_llm_prompt:\n{generate_nl_by_llm_prompt}")
    _, generated_nl, _, _ = call_gpt(generate_nl_by_llm_prompt, MODEL35)
    return generated_nl


def direct_generate_nl_by_llm(predicted_sql):
    direct_generate_nl_by_llm_prompt = '''
You are a SQL expert. 

Given the following SQL query, you need to translate it into the corresponding natural language without neglecting semantic details.

SQL:
{}

Please directly output the natural language without explanation.
'''
    direct_generate_nl_by_llm_prompt = direct_generate_nl_by_llm_prompt.format(predicted_sql)
    _, generated_nl, _, _ = call_gpt(predicted_sql, MODEL35)
    return generated_nl


def direct_generate_nl_by_llm_without_step(predicted_sql):
    direct_generate_nl_by_llm_prompt = '''
You are a SQL expert. 

Given the following SQL query, you need to translate it into the corresponding natural language summary, which should be concise.

SQL:
{}

Please directly output the natural language summary without explanation.
'''
    direct_generate_nl_by_llm_prompt = direct_generate_nl_by_llm_prompt.format(predicted_sql)
    _, generated_nl, _, _ = call_gpt(predicted_sql, MODEL35)
    return generated_nl


def increment_write_path(file_path, new_data):
    try:
        with open(file_path, 'r') as file:
            existing_data = json.load(file)
    except FileNotFoundError:
        existing_data = {}
 
    # 合并数据
    if existing_data == {}:
        existing_data = []
    existing_data.append(new_data)
 
    # 写入文件
    with open(file_path, 'w') as file:
        json.dump(existing_data, file, indent=4)



def postprocess_for_refinement(input_path, verify_output, output_path):
    with open(input_path, 'r') as input_f:
        all_data = json.load(input_f)

    with open(verify_output, 'r') as verify_f:
        verify_data = json.load(verify_f)
    
    bird_format_data = {}

    data_index = 0
    verify_index = 0
    for one_data in all_data:
        if one_data['difficulty'] != 'challenging':
            bird_format_data[str(data_index)] = ""+"\t----- bird -----\t"+one_data['db_id']
            data_index += 1
            continue

        one_verify_data = verify_data[verify_index]
        if not (one_data['question'] == one_verify_data['question'] and one_data['db_id'] == one_verify_data['db_id']):
            print("error 2")
            exit(-1)

        if 'chosed_sql' in one_verify_data.keys():
            if 'chosed_sql_revised' in one_verify_data.keys() and one_verify_data['chosed_sql_revised'] is not None:
                to_process_sql = one_verify_data['chosed_sql_revised']
                if 'final_chosed_sql_revised' in one_verify_data.keys() and one_verify_data['final_chosed_sql_revised'] is not None:
                    to_process_sql = one_verify_data['final_chosed_sql_revised']
            else:
                to_process_sql = one_verify_data['chosed_sql']

            db_id = one_verify_data['db_id']
            tmp_comb_sql = to_process_sql
            tmp_comb_sql = tmp_comb_sql.replace("\n"," ")
            tmp_comb_sql = tmp_comb_sql.replace("Sub-SQL 1"," ")
            tmp_comb_sql = tmp_comb_sql.replace("Sub-SQL 2"," ")
            tmp_comb_sql = tmp_comb_sql.replace("Sub-SQL 3"," ")
            tmp_comb_sql = tmp_comb_sql.replace("Final SQL"," ")
            tmp_comb_sql = tmp_comb_sql.replace(": "," ")
            tmp_comb_sql = tmp_comb_sql.replace("--"," ")
            tmp_comb_sql = tmp_comb_sql.replace("```"," ")
            tmp_comb_sql = tmp_comb_sql.replace("Revised SQL:","")
            tmp_comb_sql = tmp_comb_sql.replace("Revision:","")
            tmp_comb_sql = tmp_comb_sql.replace("|| ' ' ||",",")
            tmp_comb_sql = tmp_comb_sql.replace("sql"," ")
            tmp_comb_sql = tmp_comb_sql.replace("\\"," ")
            tmp_comb_sql = tmp_comb_sql.strip('"')
            tmp_comb_sql = tmp_comb_sql.strip("\n")
            tmp_comb_sql = tmp_comb_sql.strip('"')
            tmp_comb_sql = tmp_comb_sql.strip('\\')
            tmp_comb_sql = tmp_comb_sql.strip()
            bird_format_data[str(data_index)] = tmp_comb_sql+"\t----- bird -----\t"+db_id
        else:
            print('error')
            exit(-1)
        verify_index += 1
        data_index += 1

    with open(output_path, 'w') as output_f:
        json.dump(bird_format_data, output_f, indent=4)