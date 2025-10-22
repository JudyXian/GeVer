import re
import sqlparse
from typing import List, Tuple, Set, Iterator, Dict, Any, Union
from sqlparse.sql import Comparison, Identifier, IdentifierList
from sqlparse.tokens import Whitespace
from collections import namedtuple
import os
import sqlite3
import openai
import time

Token = namedtuple('Token', ['ttype', 'value'])
QUOTE_CHARS = {'`', '\'', '"'}

table_name_query = "SELECT name FROM sqlite_master WHERE type='table';"
column_type_query = "pragma table_info('%s');"
foreign_key_query = "pragma foreign_key_list('%s')"
table_schema_query = "select sql from sqlite_master where type='table' and name='%s'"
select_all_query = "SELECT * from `%s`;"

OPENAI_API_KEY = ''
MODEL4 = 'gpt-4o'
MODEL35 = 'gpt-3.5-turbo'


class LLM:
    # openai LLMs
    TEXT_DAVINCI_003 = "text-davinci-003"
    CODE_DAVINCI_002 = "code-davinci-002"
    GPT_35_TURBO = "gpt-3.5-turbo"
    GPT_4 = "gpt-4o"
    GPT_4_TURBO = "gpt-4-turbo"

    # LLMs that use openai completion api
    TASK_COMPLETIONS = [
        TEXT_DAVINCI_003,
        CODE_DAVINCI_002
    ]

    # LLMs that use openai chat api
    TASK_CHAT = [
        GPT_35_TURBO,
        GPT_4,
        GPT_4_TURBO
    ]


def get_cursor_path(sqlite_path: str):
    try:
        if not os.path.exists(sqlite_path):
            print('Openning a new connection %s' % sqlite_path)
        connection = sqlite3.connect(sqlite_path)
    except Exception as e:
        print(sqlite_path)
        raise e
    connection.text_factory = lambda b: b.decode(errors='ignore')
    cursor = connection.cursor()
    return cursor


def replace_cur_year(query: str) -> str:
    return re.sub('YEAR\s*\(\s*CURDATE\s*\(\s*\)\s*\)\s*', '2020', query, flags=re.IGNORECASE)


def exec_db_path_(sqlite_path: str, query: str) -> Tuple[str, Any]:
    print('exec_db_path_')
    query = replace_cur_year(query)
    cursor = get_cursor_path(sqlite_path)
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        cursor.connection.close()
        return 'result', result
    except Exception as e:
        cursor.close()
        cursor.connection.close()
        return 'exception', e


def exec_db_path_all(sqlite_path: str, query: str) -> Tuple[str, Any]:
    print(f"exec_db_path_all  sqlite_path:{sqlite_path}")
    query = replace_cur_year(query)

    # sqlite path 应该对应多个数据库
    db_dir = os.path.dirname(sqlite_path)
    db_paths = [os.path.join(db_dir, basename)
                for basename in os.listdir(db_dir) if '.sqlite' in basename]
    for one_db_path in db_paths:
        print(f"one_db_path:{one_db_path}")
        cursor = get_cursor_path(one_db_path)
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            cursor.connection.close()
            if len(result) > 0:
                return 'result', result
        except Exception as e:
            cursor.close()
            cursor.connection.close()
            return 'exception', e
    return 'result', []


def get_primary_keys(schema: str) -> Set[str]:
    schema_by_list = schema.split('\n')
    unique_keys = set()
    for r in schema_by_list:
        if 'primary' in r.lower():
            unique_keys.add(r.strip().split()[0].lower(
            ).strip().replace("\"", '').replace('`', ''))
    return unique_keys


def get_schema_path(sqlite_path: str, table_name: str) -> str:
    _, schema = exec_db_path_(sqlite_path, table_schema_query % table_name)
    schema = schema[0][0]
    return schema

# table name 要小写


def get_table_names_path(sqlite_path: str) -> List[str]:
    table_names = [x[0]
                   for x in exec_db_path_(sqlite_path, table_name_query)[1]]
    return table_names


def process_str_value(v: str) -> str:
    if len(v) > 0 and v[0] in QUOTE_CHARS:
        v = v[1:]
    if len(v) > 0 and v[-1] in QUOTE_CHARS:
        v = v[:-1]
    for c in QUOTE_CHARS:
        v = v.replace(c + c, c)
    return v


def extract_toks_from_comparison(comparison_node: Comparison) -> List[Token]:
    tokens = [t for t in comparison_node.tokens if t.ttype != Whitespace]
    return tokens


def rm_placeholder(s: Union[str, None]) -> Union[str, None]:
    if s is None:
        return None
    return re.sub('placeholderrare', '', s, flags=re.IGNORECASE)


def extract_info_from_comparison(comparison_node: Comparison) -> Dict[str, Any]:
    # 从 comparison node中提取 信息
    # 当前 comparison node的所有token    一般是  column op value
    tokens = extract_toks_from_comparison(comparison_node)
    left, op, right = tokens

    returned_dict = {
        'left': left,
        'op': op.value,
        'right': right
    }

    if type(left) != Identifier:  # 左边不是一个确定的值  那就是column name? 直接返回
        return returned_dict

    table = None
    # table.column的形式?
    if len(left.tokens) == 3 and re.match('^[tT][0-9]$', left.tokens[0].value) is None:
        table = left.tokens[0].value.lower()
    col = left.tokens[-1].value

    if type(right) == Identifier:  # 右边、左边都是identifier
        if len(right.tokens) == 1 and type(right.tokens[0]) == sqlparse.sql.Token:
            right_val = right.tokens[0].value
        else:
            return returned_dict
    elif type(right) == sqlparse.sql.Token:
        right_val = right.value
    else:
        return returned_dict
    # process_str_value 去掉right value 的 引号
    returned_dict['table_col'], returned_dict['val'] = (rm_placeholder(
        table), rm_placeholder(col.upper())), rm_placeholder(process_str_value(right_val))

    return returned_dict


def extract_all_comparison_from_node(node: Token) -> List[Comparison]:
    comparison_list = []
    if hasattr(node, 'tokens'):
        for t in node.tokens:
            comparison_list.extend(extract_all_comparison_from_node(t))
    if type(node) == Comparison:
        comparison_list.append(node)
    return comparison_list


def extract_all_comparison(query):
    tree = sqlparse.parse(query)[0]
    comparison_list = extract_all_comparison_from_node(tree)
    return comparison_list


def identify_group_by(sql):
    # 解析SQL语句
    parsed = sqlparse.parse(sql)
    stmt = parsed[0]
    groupby_seen = False

    groupby_lists = []
    for token in stmt.tokens:
        if groupby_seen:
            groupby_list = []
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    # print("{} {}\n".format("GROUPBY att = ", identifier))
                    # groupby_list.append(identifier.value.split(' ')[0])
                    groupby_list.append(identifier.value)
                groupby_lists.append(groupby_list)
                groupby_seen = False
            elif isinstance(token, Identifier):
                # print("{} {}\n".format("GROUPBY att = ", token))
                # groupby_list.append(token.value.split(' ')[0])
                groupby_list.append(token.value)
                groupby_lists.append(groupby_list)
                groupby_seen = False

        if token.value.upper() == "GROUP BY":
            groupby_seen = True
    return groupby_lists


def ask_completion(model, batch, temperature):
    response = openai.Completion.create(
        model=model,
        prompt=batch,
        temperature=temperature,
        max_tokens=200,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        stop=[";"]
    )
    response_clean = [_["text"] for _ in response["choices"]]
    return dict(
        response=response_clean,
        **response["usage"]
    )


def ask_chat(model, messages: list, temperature, n):
    response = openai.ChatCompletion.create(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=200,
        n=n
    )
    response_clean = [choice["message"]["content"]
                      for choice in response["choices"]]
    if n == 1:
        response_clean = response_clean[0]
    return dict(
        response=response_clean,
        **response["usage"]
    )


def ask_llm(model: str, batch: list, temperature: float, n: int):
    n_repeat = 0
    while True:
        try:
            if model in LLM.TASK_COMPLETIONS:
                # TODO: self-consistency in this mode
                assert n == 1
                response = ask_completion(model, batch, temperature)
            elif model in LLM.TASK_CHAT:
                # batch size must be 1
                assert len(batch) == 1, "batch must be 1 in this mode"
                messages = [{"role": "user", "content": batch[0]}]
                response = ask_chat(model, messages, temperature, n)
                response['response'] = [response['response']]
            break
        except openai.error.RateLimitError:
            n_repeat += 1
            print(
                f"Repeat for the {n_repeat} times for RateLimitError", end="\n")
            time.sleep(1)
            continue
        except json.decoder.JSONDecodeError:
            n_repeat += 1
            print(
                f"Repeat for the {n_repeat} times for JSONDecodeError", end="\n")
            time.sleep(1)
            continue
        except Exception as e:
            n_repeat += 1
            print(
                f"Repeat for the {n_repeat} times for exception: {e}", end="\n")
            time.sleep(1)
            continue

    return response


def do_llm(prompt):
    openai.api_key = OPENAI_API_KEY
    max_cnt = 15
    cur_cnt = 0
    while cur_cnt < max_cnt:
        try:
            res = ask_llm(MODEL4, [prompt], 0, 1)

            for sql in res["response"]:
                return prompt, sql
        except Exception as e:
            cur_cnt += 1
            print(e)
            continue
    return None, None
