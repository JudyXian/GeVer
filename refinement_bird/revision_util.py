
import dashscope
import anthropic
import openai
from http import HTTPStatus
import sqlite3
import random
import os
import time
import json

MODEL4='gpt-4o'
OPENAIKEY=''


def init_chatgpt(OPENAI_API_KEY):
    openai.api_key = OPENAI_API_KEY


def ask_chat(model, messages, temperature, n):
    response = openai.ChatCompletion.create(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=900,
                n=n
            )
    response_clean = [choice["message"]["content"] for choice in response["choices"]]
    if n == 1:
        response_clean = response_clean[0]
    return dict(
        response=response_clean,
        **response["usage"]
    )


def ask_llm(model: str, batch: list, temperature: float, n:int):
    n_repeat = 0
    while True:
        try:
            assert len(batch) == 1, "batch must be 1 in this mode"
            messages = [{"role": "user", "content": batch[0]}]
            response = ask_chat(model, messages, temperature, n)
            response['response'] = [response['response']]
            break
        except openai.error.RateLimitError:
            n_repeat += 1
            print(f"Repeat for the {n_repeat} times for RateLimitError", end="\n")
            time.sleep(1)
            continue
        except json.decoder.JSONDecodeError:
            n_repeat += 1
            print(f"Repeat for the {n_repeat} times for JSONDecodeError", end="\n")
            time.sleep(1)
            continue
        except Exception as e:
            n_repeat += 1
            print(f"Repeat for the {n_repeat} times for exception: {e}", end="\n")
            time.sleep(1)
            continue

    return response


def call_gpt(prompt):
    init_chatgpt(OPENAIKEY)
    max_cnt = 15
    cur_cnt = 0
    while cur_cnt < max_cnt:
        try:
            res = ask_llm(MODEL4, [prompt], 0, 1)
            print(f"res:{res}")
            for sql in res["response"]:
                return prompt, sql
        except Exception as e:
            cur_cnt+=1
            print(e)
            continue
    return None, None

def call_qwen(prompt):
    max_cnt = 15
    cur_cnt = 0
    while cur_cnt < max_cnt:
        try:
            messages = [{'role': 'system', 'content': 'You are a helpful assistant.'},
                {'role': 'user', 'content': prompt}]
            dashscope.api_key = 'sk-56c52df1cf3643a488383050f0026d86'
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
                # defaults to os.environ.get("ANTHROPIC_API_KEY")
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



    
def get_cursor_from_path(sqlite_path):
    try:
        if not os.path.exists(sqlite_path):
            print("Openning a new connection %s" % sqlite_path)
        connection = sqlite3.connect(sqlite_path, check_same_thread = False)
    except Exception as e:
        print(sqlite_path)
        raise e
    connection.text_factory = lambda b: b.decode(errors="ignore")
    cursor = connection.cursor()
    return cursor

def execute_sql(cursor, sql):
    cursor.execute(sql)

    return cursor.fetchall()


def execute_sql_for_error(predicted_sql, db_path):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print(f"predicted_sql:{predicted_sql}")
        cursor.execute(predicted_sql)
        predicted_res = cursor.fetchall()
        return "Correct"
    except sqlite3.Error as e1:
        print(f"e1:{e1}")
        return e1