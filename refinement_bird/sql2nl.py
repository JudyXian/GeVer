import re
from sql_metadata import Parser 
import sqlparse
import random
import string
from tqdm import tqdm
import json
import numpy as np
import copy
from verification_03_30_util import *


natural_name_flag = True

g_dict = {}

# specific codes for replacement
replace_dict = {
    '-': 'this000is333a999specific888minus777keyword',
    ':': 'this0909is111a222specific999column666symbol',
    '@': 'this444is555an333at888symbol333',
    '%': 'this999is999a999specific444percentage111symbol',
    'and': 'X123X',
    'AND': 'x123x',
    '+': 'fahgdlhglsadgkldsgsdagsd',
    '*': 'sdghkjashddsgsdgsgsg',
    '.': 'wqwevnabnaoibhwrpbhwphbwphbwpberwrb',
    '=': 'bhrobhbnrnerbponerbjerpberwpbrewrb'
}


g_schema = {} # the schema of current SQL


global g_p  # global Parser
global data_flag
data_flag = False  # just set it to False as default
high_level_explanation = []
alias_maps_tb = dict()


# a function used to determine number
def isNumber(aString):
    try:
        float(aString)
        return True
    except:
        return False


# currently only return the first encountered subexpression
def getSubExpressionBeforeNextKeyword(sql, keyword):
    sql = capitalizeKeyword(sql) #关键字大写
    keyword = keyword.upper()  # capitalizeKeyword the keyword

    p = Parser(sql)  # get the parser
    temp_flag = 0  # denote if the keyword is encountered
    sub_expression = ''
    temp_tokens = []

    possible_keywords = ['select', 'from', 'group by', 'having', 'order by', 'asc', 'desc', 'limit']

    # convert p.tokens for bug
    new_tok_list = []
    for t_tok in p.tokens:
        temp_flag2 = False # used to indicate if exception encountered
        for temp_keyword in possible_keywords:
            if temp_keyword.lower() != t_tok.value.lower() and t_tok.value.lower().endswith(temp_keyword.lower()): #t_tok.value 是否是以temp_keyword结尾
                temp_list = []
                var = t_tok.value.lower().replace(temp_keyword.lower(), '') #说明关键字没有分开?
                temp_list.append(var)
                temp_list.append(temp_keyword)
                new_tok_list += temp_list
                temp_flag2 = True
                break
        if not temp_flag2:
            new_tok_list.append(t_tok.value)

    if keyword in sql.upper():
        for tok in new_tok_list:
            if tok.lower() == keyword.lower():
                temp_flag = 1
                temp_tokens.append(tok)
                continue
            if temp_flag == 1 and tok.lower() in possible_keywords:  #说明到了group by 后面出现的关键字 这时候退出
                break
            if temp_flag == 1:
                temp_tokens.append(tok)

        sub_expression = ' '.join(temp_tokens) #temp_tokens 存的是 group by以及 对应group by的column

    return sub_expression

# get the description of nouns
# should input a sub sql expression
# 生成对 sub_sql  的自然语言描述
def getNouns(sub_sql, ori_sql):
    # all_possible_keywords = [')', '(', 'between', '!=', '>=', '<=', '<', '>', '=', ',', 'select', 'from', 'as', 'join',
    #                          'on', 'where', 'and', 'or', 'group by', 'having', 'order by', 'asc', 'desc']  #考虑在这里加cast?

    all_possible_keywords = ['select', 'from', 'join', 'on', 'where'] 
    no_the_keywords = ['as', 'case', 'when', 'then', 'else', 'end']

    # judge if it is a number(for natural condition. e.g. age > 2.5)
    if isNumber(sub_sql):  #如果只是一个数字就直接返回
        return sub_sql

    if ' ' not in sub_sql and 'X123X' in sub_sql.upper(): #说明这时候是between xxx and xxx   或者是单个 word
        return sub_sql

    # the
    the_pos = ''
    if ' between ' not in sub_sql.lower():  #' distinct ' not in sub_sql.lower() and  去掉distinct相关的判断
        the_pos = 'the '

    # quoted noun is adj. (no the)
    if 'a999specific999start999symbol' in sub_sql.lower():
        the_pos = ''

    g_p = Parser(ori_sql)
    temp_subexpression = sub_sql
    while temp_subexpression.count('(') < temp_subexpression.count(')'):
        temp_subexpression = '(' +temp_subexpression
    if 'SELECT' in sub_sql.upper() or 'WHERE' in sub_sql.upper() or 'FROM' in sub_sql.upper():
        print(f"getNouns error: SELECT/ FROM/ WHERE cannot be in sub_sql.  sub_sql:{sub_sql}")
        raise ValueError(f"getNouns error: SELECT/ FROM/ WHERE cannot be in sub_sql.  sub_sql:{sub_sql}")
        # exit(-1)
    temp_subexpression = 'SELECT ' + temp_subexpression  # adding SELECT is for ignoring the exception    
    p1 = Parser(temp_subexpression)  # get the parser for subexpression
    # CASE WHEN xx THEN xx ELSE xx END 是否匹配   CASE 和 END个数必须匹配
    if temp_subexpression.count('CASE') != temp_subexpression.count('END'):
        print(f"error . case_num:{temp_subexpression.count('CASE')}  and end_num:{temp_subexpression.count('END')}  are not the same.")
        print(f"temp_subexpression:{temp_subexpression}")
        raise ValueError(f"error . case_num:{temp_subexpression.count('CASE')}  and end_num:{temp_subexpression.count('END')}  are not the same.")
        # exit(-1)

    # lower possible function names
    functionNames = ['count', 'max', 'min', 'avg', 'sum', 'cast', 'substr', 'substring', 'strftime','date','instr'] #jzx TODO 是否还有别的functions  #考虑在这里加cast?
    cal_operators = {
        '+': 'add',
        '-': 'minus',
        '*': 'multiply',
        '/': 'divide'
                     } #加减乘除 符号前后不能分开 并且用括号括起来
    number_type = ['INTEGER', 'REAL']
    temp_explanation = ''

    # find all functions and the corresponding paramenters
    func_start_list = [] #记录每个函数的开始 和结束的索引 以便后面不再遍历
    func_end_list = []
    paras = []
    temp_paras = []

    # find the starting position of parameters (after '(' )
    positions = []
    length = len(p1.tokens)
    
    # jzx TODO: sub_sql 可能存在嵌套的吗?  getNouns() 不应该都消灭掉了吗
    for i in range(0, length):
        #xyr
        if p1.tokens[i].value == '(' and p1.tokens[i].parenthesis_level == 1 and  (i+1)<len(p1.tokens) and p1.tokens[i+1].value.lower() != 'select' and \
            (i-1)>=0 and p1.tokens[i-1].value.lower() in functionNames: #左括号(前面必须是函数才行
            positions.append(i)
    # 记录sub_sql 存在的函数及其 参数位置

    positions1 = [i + 1 for i in positions]  # move from '(' to the index of parameter  参数的位置
    funcs = []
    # get function names (right in front of '(' )  记录函数名
    for pos in positions:
        funcs.append(p1.tokens[pos - 1].value.upper())
        func_start_list.append(pos-1)
    # get parameter for this function  函数的参数里面也可能有 括号--> 递归  但是括号里面不可能存在 SELECT子句了  ---> decompose 要递归地处理所有的SELECT子句
    for pos in positions1: #遍历参数 如果 有反括号 ) 应该用递归
        temp_paras = []
        j = pos  #这里应该加个递归
        while not (p1.tokens[j].value == ')' and p1.tokens[j].parenthesis_level == 0):
            # if p1.tokens[j].value != ',':
            # p1.tokens[j].value 里面可能会出现别名
            tmp_p = copy.deepcopy(str(p1.tokens[j].value))
            # temp_paras.append(copy.deepcopy(tmp_p)) #函数里面的参数
            for alias, ori in alias_maps_tb.items():  
                tmp_p = tmp_p.replace(alias, ori) #参数里面的table 别名
            # temp_paras.append(p1.tokens[j].value)
            temp_paras.append(tmp_p)
            j += 1
        func_end_list.append(j)
        sql_temp_paras = ' '.join(temp_paras)  #把括号里面的子句生成自然语言
        noun_sql_temp_paras = getNouns(sql_temp_paras, ori_sql)
        paras.append(noun_sql_temp_paras) #参数这里转成自然语言了

    # generate nouns
    nouns = []
    noun = ''
    for i in range(0, len(funcs)):
        noun = ''
        # if i != 0:
        #     noun += ', '

        if funcs[i] == 'COUNT':
            if paras[i] == '*':
                noun += 'number of records'
            else:
                noun += 'number of '
        elif funcs[i] == 'MAX':
            noun += 'maximum value of '
        elif funcs[i] == 'MIN':
            noun += 'minimum value of '
        elif funcs[i] == 'AVG':
            noun += 'average value of '
        elif funcs[i] == 'SUM':
            noun += 'sum of '
        elif funcs[i] == 'CAST': #TODO
            noun += 'regarding data of'
        elif funcs[i] == 'STRFTIME':
            noun += 'extracting date time from'
        elif funcs[i] == 'SUBSTR' or funcs[i] == 'SUBSTRING':
            noun += 'extracting string from'
        elif funcs[i] == 'DATE':
            noun += 'date of'
        elif funcs[i] == 'INSTR':
            noun += 'find substring index from'
        # paras 存的是参数的自然语言 表示  所以都是一维list
        # for j in range(0, len(paras[i])):
        #     if j != 0:
        #         noun += ' '
        #     if paras[i][j] != '*':
        #         noun += paras[i][j]
        noun += paras[i] # 函数+参数 对应的 自然语言

        nouns.append(noun)

    # add column not included in functions to nouns
    i = 1
    # for i in range(1, len(p1.tokens)):
    # case_when = False
    while  i < len(p1.tokens):  #处理除了 函数之外的信息
        # print(f"1:{p1.tokens[i].value.lower()}   2:{p1.tokens[i].parenthesis_level}")
        # [func_start_list, func_end_list] --> func(xxx)的索引
        # 如果 函数后面有别名
        for func_index in range(len(func_start_list)):
            if i == func_end_list[func_index]: #函数的最后一个索引
                # 考虑别名  AS 后面不是 real integer这些
                if (i+1)<len(p1.tokens) and p1.tokens[i+1].value.lower() == 'as' and (i+2)<len(p1.tokens) and p1.tokens[i+2].value not in number_type:
                    alias_f = p1.tokens[j+2].value
                    # 感觉不用这个就可以 alias_maps_func
                    # nouns 到这里nouns只是存的 function相关的 NL
                    nouns[func_index] += f", which is denoted as `{alias_f}` "
                    i += 3

        is_in_func = False
        for func_index in range(len(func_start_list)):
            if i >= func_start_list[func_index] and i <= func_end_list[func_index]: #在函数的索引范围内 就不处理
                is_in_func = True
                break
        if is_in_func:
            i += 1
            continue
        if i == len(p1.tokens):
            break

        # 先处理递归的括号
        # print(f"i:{i}  len of p1:{len(p1.tokens)}.   p1:{p1.query}")
        # print(f"func_start_list:{func_start_list}")
        # print(f"func_end_list:{func_end_list}")
        # print(f"funcs:{funcs}")
        if p1.tokens[i].value.lower() == '(' and p1.tokens[i].parenthesis_level == 1: #这里加一个递归调用  因为这里的column 可能被括号括起来了， 但也是正常的column 所以需要被解析
            # if (i-1) >= 0 and p1.tokens[i-1].value.lower() in functionNames: #如果这里是函数+括号() 则不需要递归 因为前面已经处理过了  --> 到了这里就不会出现函数了
            #     i+=1
            #     continue
            sub_p1 = []
            i += 1
            # print(f"p1.query:{p1.query}")
            while not (p1.tokens[i].value.lower() == ')' and p1.tokens[i].parenthesis_level == 0):
                sub_p1.append(p1.tokens[i].value)
                i += 1
            sub_sql_p1 = ' '.join(sub_p1)
            # print(f"sub_sql_p1:{sub_sql_p1}")
            # print(f"func_start_list:{func_start_list}")
            # print(f"func_end_list:{func_end_list}")
            # print(f"funcs:{funcs}")
            tmp_nouns = getNouns(sub_sql_p1, ori_sql) #对括号里面的递归处理  这时候 不是函数的括号
            nouns.append(tmp_nouns) #后面处理的就是非括号了   前面所有的括号都处理完了
        # case when 应该在这里chuli
        elif (i+1) < len(nouns) and 'CASE' == nouns[i]: #处理case when 的情况 这里处理的可能有点问题 我们假设都是 case when xxx then 1 else 0 end  所以去掉
            #先假设格式都是这样吧  CASE WHEN gender = 'M' THEN 1 ELSE 0 END  后面有问题再说
            i += 1
            sub_p2 = []
            # CASE WHEN 和 THEN 之间的 也就是要满足的条件
            case_when_nouns = 'if it maintains '
            while 'END' != p1.tokens[i].value:
                sub_p2.append(p1.tokens[i].value)
                i += 1
            sub_sql_p2 = ' '.join(sub_p2)
            tmp_noun = getNouns(sub_sql_p2, ori_sql)
            case_when_nouns += tmp_noun
            nouns.append(case_when_nouns)
        elif p1.tokens[i].value == '*' and p1.tokens[i].parenthesis_level == 0:
            nouns.append('all the records')
        elif p1.tokens[i].is_integer and p1.tokens[i].parenthesis_level == 0:
            nouns.append(p1.tokens[i].value)
        elif p1.tokens[i].value.lower() not in all_possible_keywords and p1.tokens[i].parenthesis_level == 0:
            # 这里也需要对别名做替换处理
            ret_column = copy.deepcopy(str(p1.tokens[i].value))  #返回列 也可能存在别名
            for alias, ori_col in alias_maps_tb.items():
                ret_column = ret_column.replace(alias, ori_col)
            # nouns.append(p1.tokens[i].value)
            nouns.append(ret_column)
        i+=1


    # alias.Y ---> Y of X  处理自然语言中的别名
    for i in range(0, len(nouns)): 
        tok = nouns[i]
        if '.' in tok:
            temp_tok = tok.split('.')
            if len(temp_tok) != 2:
                print("exception: . should split it into 2 parts")
                break
            if temp_tok[0] in g_p.tables_aliases.keys():
                table_name = g_p.tables_aliases[temp_tok[0]]
            else:
                table_name = temp_tok[0]
            # hold the comma
            comma = ''
            if ',' in temp_tok[1]:
                comma = ','
                temp_tok[1] = temp_tok[1].replace(',', '')
            # Y of X
            res_str = '`'+temp_tok[1] +'`'+ ' of ' +'`'+ table_name +'`'+ comma
            nouns[i] = res_str
        # else:
        #     res_str = '`'+temp_tok +'`'

    # add nouns to the explanation
    # X, Y, Z --> the X, the Y, and the Z
    i = 0    
    while i < len(nouns):
        if (i+1) < len(nouns) and (i+2) < len(nouns) and nouns[i+1] in cal_operators.keys(): #处理计算操作
            #如果前面一个符号是 + - * / 就将符号与其对应的操作数放在一起 然后用括号括起来      暂时不考虑负号
            if i == 0:
                temp_explanation += ' (' #此时肯定不是最好一个字符
            elif (i+2) == len(nouns) - 1:
                temp_explanation += ', and ('
            else:
                temp_explanation += ', ('
            temp_explanation += (nouns[i] + ' ' + cal_operators[nouns[i+1]] +' '+ nouns[i+2]+') ') #不要提percentage 和 ratio了
            i += 3
        if i >= len(nouns): #这时数组已经结尾了 就终止
            break
        if nouns[i] in number_type:
            temp_explanation += (nouns[i].lower() + ' number ')
            i += 1
        if i >= len(nouns): #这时数组已经结尾了 就终止
            break
        if i != 0: #column  所有and前面都得有逗号,
            if i == len(nouns)-1:
                temp_explanation += ', and'
            elif nouns[i].lower() not in no_the_keywords: #如果是 as when then这些关键字 后面不用加,
                temp_explanation += ', '

        if 'all the records' not in nouns[i].lower() and nouns[i].lower() not in no_the_keywords: #如果是 as when then这些关键字 后面不用加 the
            temp_explanation += ' ' + the_pos  #这里会出现很多的the
        else:
            temp_explanation += ' '
        temp_explanation += nouns[i]
        i += 1
    #这里不可能有 子句  但是可能会存在括号，所以需要递归调用

    return temp_explanation


def NLforOperator(clause): #把一些符号 变成自然语言
    if '>=' in clause:
        clause = clause.replace('>=', 'is greater than or equal to')
    if '<=' in clause:
        clause = clause.replace('<=', 'is less than or equal to')
    if '>' in clause:
        clause = clause.replace('>', 'is greater than')
    if '<' in clause:
        clause = clause.replace('<', 'is less than')
    if '!=' in clause:
        clause = clause.replace('!=', 'is not')
    if '=' in clause:
        clause = clause.replace('=', 'is')
    if ' NOT IN ' in clause:
        clause = clause.replace(' NOT IN ', ' is not in ')
    elif ' not in ' in clause:
        clause = clause.replace(' not in ', ' is not in ')
    elif ' IN ' in clause:
        clause = clause.replace(' IN ', ' is in ')
    elif ' in ' in clause:
        clause = clause.replace(' in ', ' is in ')
    if ' between ' in clause:
        clause = clause.replace(' between ', ' is between ')
    elif ' BETWEEN ' in clause:
        clause = clause.replace(' BETWEEN ', ' is between ')
    if ' NOT LIKE ' in clause:
        clause = clause.replace(' NOT LIKE ', ' is not in the form of ')
    elif ' not like ' in clause:
        clause = clause.replace(' not like ', ' is not in the form of ')
    elif ' not LIKE ' in clause:
        clause = clause.replace(' not LIKE ', ' is not in the form of ')
    elif ' LIKE ' in clause:
        clause = clause.replace(' LIKE ', ' is in the form of ')
    elif ' like ' in clause:
        clause = clause.replace(' like ', ' is in the form of ')
    elif ' IS NOT NULL ' in clause:
        clause = clause.replace(' IS NOT NULL ',' cannot be null ')

    return clause



def capitalizeKeyword(sql):
    p = Parser(sql)
    keywords = ['between', 'like', 'select', 'from', 'as', 'join', 'on', 'where', 'and', 'or', 'group by', 'having', 'not'
                'order by', 'asc', 'desc', 'avg', 'count', 'max', 'min', 'with', 'strftime', 'substr', 'substring', 'cast', 'real']

    sql_tokens = sql.split()

    tokens = []  # result list
    for i, tok in enumerate(sql_tokens):
        # capitalize "order by", "group by" (keyword composed of multiple words)
        if tok.lower() == 'order' or tok.lower() == 'group':
            if i != len(sql_tokens) - 1 and sql_tokens[i + 1].lower() == 'by':
                tokens.append(tok.upper())
        elif tok.lower() == 'by' and i != 0:
            if sql_tokens[i - 1].lower() == 'order' or sql_tokens[i - 1].lower() == 'group':
                tokens.append(tok.upper())
        elif tok in keywords:
            tokens.append(tok.upper())  # capitalize keyword token
        else:
            tokens.append(tok)

    res = ' '.join(tokens)

    return res



def reorganize_explanations():
    # directly modify the high_level_explanation
    global high_level_explanation
    new_high_level_explanation = copy.deepcopy(high_level_explanation)  #这里存着 decompose的结果

    for query_idx, exp_unit in enumerate(new_high_level_explanation):  #当前query 的所有解释信息
        #为啥会有0 和1
        # print(f"exp_unit:{exp_unit}")
        if isinstance(exp_unit['explanation'], str):
            continue

        sub_list = exp_unit['explanation'][0]
        print(f"sub_list:\n{sub_list}")
        exp_list = exp_unit['explanation'][1]
        # exit(-1)

        Explanation_unit = {
            'from': {
                'sub': '',
                'exp': ''
            },
            'where': {
                'sub': '',
                'exp': ''
            },
            'group': {
                'sub': '',
                'exp': ''
            },
            'having': {
                'sub': '',
                'exp': ''
            },
            'order': {
                'sub': '',
                'exp': ''
            },
            'select': {
                'sub': '',
                'exp': ''
            }
            # 'union':{
            #     'sub': '',
            #     'exp': '',
            # },
            # 'intersect':{
            #     'sub': '',
            #     'exp': '',
            # },
            # 'except':{
            #     'sub': '',
            #     'exp': '',
            # }
        }

        for idx, (sub, exp) in enumerate(zip(sub_list, exp_list)):
            # temp process
            exp = exp.replace(' SELECT ', ' select ') #解释信息全部变成小写
            exp = exp.replace(' FROM ', ' from ')
            exp = exp.replace(' TABLE ', ' table ')
            exp = exp.replace(' WHERE ', ' where ')
            exp = exp.replace(' ORDER ', ' order ')
            exp = exp.replace(' GROUP ', ' group ')
            exp = exp.replace(' BY ', ' by ')

            # separate ``select ...`` and ``from ...``
            if sub.lower().startswith('select '):
                # original pattern `` Show ... from TABLE ... ``
                temp_exp = exp.replace(' from table ', ' from TABLE ')
                temp_exp = temp_exp.replace(' FROM TABLE ', ' from TABLE ')
                temp_exp = temp_exp.replace(' FROM table ', ' from TABLE ')
                temp_sub = sub.replace(' from ', ' FROM ')

                if ' from TABLE ' in temp_exp:
                    # explanation
                    select_exp = temp_exp.split(' from TABLE ', 1)[0]
                    from_exp = 'In table ' + temp_exp.split(' from TABLE ', 1)[1]  #from table 的解释 改成 In table 这样显得自然语言更加自然?

                    # subexpression
                    select_sub = temp_sub.split(' FROM ')[0]
                    from_sub = 'FROM ' + temp_sub.split(' FROM ')[1]

                    # set
                    Explanation_unit['select']['sub'] = select_sub
                    Explanation_unit['select']['exp'] = select_exp

                    Explanation_unit['from']['sub'] = from_sub
                    Explanation_unit['from']['exp'] = from_exp

                else:
                    raise Exception('Cannot separate to from clause and select clause')

            elif sub.lower().startswith('group by '):
                if ' having ' not in sub.lower():
                    Explanation_unit['group']['exp'] = exp
                    Explanation_unit['group']['sub'] = sub
                else:
                    temp_exp = exp
                    temp_sub = sub.replace(' having ', ' HAVING ')

                    # explanation
                    group_exp = temp_exp.split(' where ', 1)[0]
                    # xyr
                    # if 'where' in temp_exp:
                    #     having_exp = 'Keep the groups where ' + temp_exp.split(' where ', 1)[1]
                    # else:
                    # print(sub)
                    # print(exp)
                    having_exp = 'Keep the groups where ' + temp_exp.split(' where ', 1)[1]
                    print('&&&')
                    print(having_exp)

                    # subexpression
                    group_sub = temp_sub.split(' HAVING ')[0]
                    having_sub = 'HAVING ' + temp_sub.split(' HAVING ')[1]

                    # set
                    Explanation_unit['group']['sub'] = group_sub
                    Explanation_unit['group']['exp'] = group_exp

                    Explanation_unit['having']['sub'] = having_sub
                    Explanation_unit['having']['exp'] = having_exp

            elif sub.lower().startswith('order by '):
                # if `count (*)`: sort the records ---> sort the groups
                temp_sub = sub.lower()
                temp_sub = re.sub(r' *\( *', '(', temp_sub)
                temp_sub = re.sub(r' *\) *', ') ', temp_sub)
                if 'count(*)' in temp_sub:
                    temp_exp = exp.replace('Sort the records', 'Sort the groups')
                else:
                    temp_exp = exp

                # set
                Explanation_unit['order']['sub'] = sub
                Explanation_unit['order']['exp'] = temp_exp

            elif sub.lower().startswith('where '):
                # set
                Explanation_unit['where']['sub'] = sub  #where的语句不用重新组织
                Explanation_unit['where']['exp'] = exp
            # elif 'union' in sub.lower():
            #     Explanation_unit['union']['sub'] = sub  #where的语句不用重新组织
            #     Explanation_unit['union']['exp'] = exp
            # elif 'intersect' in sub.lower():
            #     Explanation_unit['intersect']['sub'] = sub  #where的语句不用重新组织
            #     Explanation_unit['intersect']['exp'] = exp
            # elif 'except' in sub.lower():
            #     Explanation_unit['except']['sub'] = sub  #where的语句不用重新组织
            #     Explanation_unit['except']['exp'] = exp

        # update the explanation and subexpressions
        new_sub_list = []
        new_exp_list = []
        new_llm_exp_list = [] #让大模型生成的nl解释信息
        # type_order = ['from', 'where', 'group', 'having', 'order', 'select', 'union', 'intersect', 'except']
        type_order = ['from', 'where', 'group', 'having', 'order', 'select']
        for type in type_order:
            if not Explanation_unit[type]['sub']:
                continue
            new_sub_list.append(Explanation_unit[type]['sub'])
            new_exp_list.append(Explanation_unit[type]['exp'])
            tmp_llm_exp = ""
            # 大模型的prompt也要把 alias map输入进去
            # 所有类型的子句 都用一个prompt吧
            # if type == 'from' or type == 'select': #如果是from 或者 select 就参考step生成的结果
            #     pass
            # else:
            #     pass
            tmp_llm_exp = generate_nl_by_llm(Explanation_unit[type]['sub'], Explanation_unit[type]['exp'], alias_maps_tb)
            print(f"tmp_llm_exp:{tmp_llm_exp}")
            new_llm_exp_list.append(tmp_llm_exp)

        if not isinstance(high_level_explanation[query_idx]['explanation'], str):
            # print(f"high_level_explanation[query_idx]:{high_level_explanation[query_idx]}")
            high_level_explanation[query_idx]['explanation'] = [copy.deepcopy(new_sub_list), copy.deepcopy(new_exp_list), copy.deepcopy(new_llm_exp_list)]
            # high_level_explanation[query_idx]['explanation'] = str(high_level_explanation[query_idx]['explanation'])
            # high_level_explanation[query_idx]['explanation'][1] = copy.deepcopy(new_exp_list)


def decompose(sql):
    global g_subSQL
    global high_level_explanation
    global g_dict
    global replace_dict

    sql = preprocessSQL(sql) #有点冗余

    p = Parser(sql)  # get the parser
    # construct token list of Parser(sql)
    # print(f"decomposeSQL:{sql}")
    token_list = []
    for tok in p.tokens:
        token_list.append(tok.value)
    # print(f"token_list:{token_list}")

    # construct the temp dict for high level explanation   解释信息存储的临时变量
    temp_explanation_dict = {'number': '', 'subquery': '', 'explanation': '', 'supplement': ''}

    # 分解 union intersect等并列关系的子句
    for i in range(0, len(p.tokens)):
        # 应该先根据with 子句 进行分解
        # 通过两个并列的子句分割
        if p.tokens[i].parenthesis_level == 0 and (
                p.tokens[i].value.upper() == 'INTERSECT' or p.tokens[i].value.upper() == 'UNION' or p.tokens[i].value.upper() == 'EXCEPT' or p.tokens[i].value.upper() == 'UNION ALL'):
            pos = i  # divide 2 sub-queries from this token

            # left sub-sql   并列左子句
            sql1 = ' '.join(token_list[0:pos]) #不包括 pos位置
            while sql1[0] == '(' and sql1[-1] == ')': #去掉子句两边的括号
                sql1 = sql1.strip('()')  # delete extra parenthesis
            # right sub-sql
            sql2 = ' '.join(token_list[pos + 1:])
            while sql2[0] == '(' and sql2[-1] == ')':
                sql2 = sql2.strip('()')  # delete extra parenthesis
            # recursively decompose  递归分解每个子句
            res1 = decompose(sql1)  # res: the xxth query result
            res2 = decompose(sql2)

            # construct result (which query this is)  自然语言表示 每个结果
            result = num2ordinalStr(len(high_level_explanation) + 1) + ' query result'
            
            print('res1:', res1)
            print('res2:', res2)
            print(p.tokens[i].value.lower())
            
            # construct replaced/modified subquery
            modified_subquery = res1 + ' ' + p.tokens[i].value.lower() + ' ' + res2

            # construct the explanation  构造自然语言解释   jzx TODO: 这里可以用LLM做生成
            if p.tokens[i].value.upper() == 'INTERSECT':
                content = 'Keep the intersection of ' + res1 + ' and ' + res2 + '.'
            elif p.tokens[i].value.upper() == 'UNION' or p.tokens[i].value.upper() == 'UNION ALL':
                content = 'Keep the union of ' + res1 + ' and ' + res2 + '.'
            elif p.tokens[i].value.upper() == 'EXCEPT':
                content = 'Keep the records in ' + res1 + ' but not in ' + res2 + '.'

            temp_explanation_dict['number'] = result
            temp_explanation_dict['subquery'] = modified_subquery
            temp_explanation_dict['explanation'] = content
            # print(f"temp_explanation_dict['subquery']:{modified_subquery}")

            # add it to high level explanation list
            high_level_explanation.append(temp_explanation_dict)
            # print(f"high_level_explanation:\n{high_level_explanation}")

            return result

    # if situation 1 is not satisfied
    # 2. find ... (select ...)
    # need to parse sql
    modified_subquery = sql
    xyr_tmp_supplement = ""
    xyr_nest_flag = 0
    # 这时候 不包括 union/except/intersect连接的子查询
    for i in range(0, len(p.tokens)):  #判断是否有子查询
        if p.tokens[i].value.upper() == 'SELECT' and p.tokens[i].parenthesis_level == 1 and p.tokens[
            i].previous_token.value == '(': #子查询语句  SELECT前面必须是(
            xyr_nest_flag = 1
            pos = i
            # get the end pos
            j = i
            temp_tok = p.tokens[i]
            while True:
                temp_tok = temp_tok.next_token
                j += 1
                if temp_tok.value == ')' and temp_tok.parenthesis_level == 0:
                    end_pos = j #子查询结束
                    break
                if temp_tok.next_token.position == -1:  #SQL出错
                    print("Exception: No matched )")
                    assert 1==0
                    break

            sql1 = ' '.join(token_list[pos:end_pos])  #截取子查询
            # print(f"hhhh:sql1:{sql1}")
            # 该子串在整个串里第几次出现
            while sql1[0] == '(' and sql1[-1] == ')':
                sql1 = sql1.strip('()')  # delete extra parenthesis
            if sql1.upper().count('SELECT') > 1: #这个子查询里面可能还有子查询:
                # 多个子查询直接返回失败吧
                return 'multiple sub-queries, return false'
            else:
                res1 = decompose(sql1)  #解析这个子查询

            # construct result (which query this is)
            result = num2ordinalStr(len(high_level_explanation) + 1) + ' query result'

            # construct replaced/modified subquery
            temp_concatenate_res_str = res1.replace(' ',
                                                    '123')  # to prevent this string is broken when parse the sql, and replace back later
            sql = sql.replace(' ,',',')
            modified_subquery = re.sub(r' *\( *', '(', modified_subquery) #modified_subquery对应原始查询   去掉sql中(/)前面的空格
            modified_subquery = re.sub(r' *\) *', ') ', modified_subquery)

            #去掉所有左右括号前面的空格
            sql1 = re.sub(r' *\( *', '(', sql1)
            sql1 = re.sub(r' *\) *', ') ', sql1)
            # xyr
            sql1 = sql1.replace(' ,',',')
            sql1 = sql1.replace(' )',')')
            sql1 = sql1.replace('( ','(').strip()
            
            # temp_sql = sql
            # xyr
            temp_sql = modified_subquery #原始sql
            temp_sql = re.sub(r' *\( *', '(', temp_sql)
            temp_sql = re.sub(r' *\) *', ') ', temp_sql)
            temp_sql = temp_sql.replace(' ,',',')
            temp_sql = temp_sql.replace(') )','))')
            temp_sql = temp_sql.replace(')) )',')))')
            # 所有连续右括号 )) 之间的空白字符，使它们变成紧密连接的 ))
            temp_sql = re.sub(r'\)\s+\)', '))', temp_sql)
            temp_sql = re.sub(' +', ' ', temp_sql) 
            if '('+sql1+')' in temp_sql:
                modified_subquery = temp_sql.replace('('+sql1+')', '('+temp_concatenate_res_str+')',1) #子查询内容替换成result 这样后面就不用再解析了
                # print(f"modified:{modified_subquery}")
            else:
                #解析失败
                print(f"temp_sql:{repr(temp_sql)}")
                print(f"sql1:{sql1}")
                raise ValueError('temp_concatenate_res_str not in the original sql')

            modified_subquery = modified_subquery.replace('(', ' ( ')
            modified_subquery = modified_subquery.replace(')', ' ) ')
            # delete ’(' and ')' of modified_subquery and double check whether if the parenthesis include the subquery
            temp_str_tok_l = modified_subquery.split()
            # print(f"modified_subquery:{modified_subquery}")
            # print(f"temp_str_tok_l:{temp_str_tok_l}")
            try:
                sub_len = len(['(',temp_concatenate_res_str,')'])
                for i in range(len(temp_str_tok_l) - sub_len + 1):
                    if temp_str_tok_l[i:i + sub_len] == ['(',temp_concatenate_res_str,')']:
                        temp_index = i + 1
            except Exception as e:
                pass
            if temp_str_tok_l[temp_index - 1] == '(' and temp_str_tok_l[temp_index + 1] == ')': #去掉子查询对应的括号
                # delete the corresponding '(' and ')'
                del temp_str_tok_l[temp_index + 1]
                del temp_str_tok_l[temp_index - 1]
            else:
                print(temp_str_tok_l[temp_index - 1])
                print(temp_str_tok_l[temp_index + 1])
                print(temp_str_tok_l)
                print(temp_concatenate_res_str)
                raise Exception("Exception: the left of subquery should be ( and the right of subquery should be )")

            # get the modified_subquery without '(', ')'
            modified_subquery_new = ' '.join(temp_str_tok_l)
            # parse the sql and get the structured explanantion
            # 这时的 modified_subquery_new 应该不包括子查询
            if modified_subquery_new.upper().count('SELECT') > 1: #这个子查询里面可能还有子查询:
                # 多个子查询直接返回失败吧
                return 'multiple sub-queries, return false'
            content = parseSQL(modified_subquery_new) #对原始sql (此时已经处理完sql中的子sql了) 生成解释信息
            # xyr
            for i in range(0, len(content[0])):
                if data_flag: #False
                    content[0][i] = content[0][i].replace(temp_concatenate_res_str, '(1)') # for generating data
                else:
                    sql1_1 = sql1.replace('a999specific999start999symbol', '') #后处理  去掉一些特殊符号
                    sql1_1 = sql1_1.replace('9999this0is0a0specific0hypen123654777', '')

                    sql1_1 = sql1_1.replace('a753275specific37575start37397symbol', '') #后处理  去掉一些特殊符号
                    sql1_1 = sql1_1.replace('786786his0is0a0sp7867867en1236783737', '')

                    #xyr
                    content[0][i] = content[0][i].replace('123query123result', '_query_result')
            if xyr_tmp_supplement != "":
                xyr_tmp_supplement += ', '+res1  #有子查询 时用到
            else:
                xyr_tmp_supplement = ' uses ' + res1

    if xyr_nest_flag==1:
        for i in range(0, len(content[1])): #content[1]代表解释信息
            content[1][i] = content[1][i].replace(temp_concatenate_res_str, res1)  #子查询指向对应的 解释的索引
        temp_explanation_dict['number'] = result
        temp_explanation_dict['subquery'] = modified_subquery_new
        temp_explanation_dict['explanation'] = content
        temp_explanation_dict['supplement'] = result + xyr_tmp_supplement #xyr_tmp_supplement 标记用到了子查询的解释
        # add it to high level explanation list
        high_level_explanation.append(temp_explanation_dict)
        print(high_level_explanation)
        return result

    # otherwise, there should no further high level
    # directly regard it as atomic sql
    # double check:
    temp_cnt = sql.count('SELECT') + sql.count('select')
    if temp_cnt != 1:
        print("Exception: there should only 1 SELECT in the atomic query sentence!")
        return

    # construct result (which query this is)
    result = num2ordinalStr(len(high_level_explanation) + 1) + ' query result'
    # parse the sql and get the structured explanantion
    # print(f"parseSQL:{sql}")
    content = parseSQL(sql)

    # recover SQL
    temp_tok_list = sql.split()
    for k in range(len(temp_tok_list)): #sql中去掉之前加入的特殊字符
        if 'a999specific999start999symbol' in temp_tok_list[k]:
            temp_tok_list[k] = '\"' + temp_tok_list[k] + '\"'
            temp_tok_list[k] = temp_tok_list[k].replace('a999specific999start999symbol', '')
            temp_tok_list[k] = temp_tok_list[k].replace('9999this0is0a0specific0hypen123654777', ' ')
            for key in replace_dict.keys():
                temp_tok_list[k] = temp_tok_list[k].replace(replace_dict[key], key)
        
        if 'a753275specific37575start37397symbol' in temp_tok_list[k]:
            temp_tok_list[k] = '`' + temp_tok_list[k] + '`'
            temp_tok_list[k] = temp_tok_list[k].replace('a753275specific37575start37397symbol', '')
            temp_tok_list[k] = temp_tok_list[k].replace('786786his0is0a0sp7867867en1236783737', ' ')
            for key in replace_dict.keys():
                temp_tok_list[k] = temp_tok_list[k].replace(replace_dict[key], key)

    sql = ' '.join(temp_tok_list)

    temp_explanation_dict['number'] = result
    temp_explanation_dict['subquery'] = sql  # doesn't change
    temp_explanation_dict['explanation'] = content

    # add it to high level explanation list
    high_level_explanation.append(temp_explanation_dict)
    return result


def generateRandomLetterString():
    #随机生成一个长度为2-12的字符串
    sql_keywords = ['in','between', 'like', 'not','by','group','order','with','strftime',  #of 不是关键字吧 去掉
                    'substr','substring','date','min','avg','max','count','asc','desc','having',
                    'and','or','union','intersect','select','from','where','cast', 'real','as']
    ran_num = random.randint(2, 12) # length: 2-12
    generated_str = ''
    for i in range(ran_num):
        ran_str = ''.join(random.sample(string.ascii_letters, 1))
        generated_str += ran_str

    # the random string shouldn't be SQL keyword
    parsed = sqlparse.parse(generated_str) #生成的字符串不能包含SQL 关键字
    while parsed[0].tokens[0].is_keyword or generated_str.lower() in sql_keywords:  #jzx TODO: 增加更多关键字 以及一些关键的函数  --> DONE
        generated_str = ''
        for i in range(ran_num):
            ran_str = ''.join(random.sample(string.ascii_letters, 1))
            generated_str += ran_str

        parsed = sqlparse.parse(generated_str)

    generated_str = generated_str.lower()
    return generated_str

def preprocessSQL(sql):
    # xyr_preprocess 去掉一些空格
    sql = sql.replace('( ','(')
    sql = sql.replace(' )',')')
    sql = sql.replace(' ,',',')
    #

    global g_dict
    global replace_dict
    sql = sql.replace('\'', '\"')

    # 将多个空格 替换为一个空格
    # print(f"before sql: {sql}")
    sql = re.sub(' +', ' ', sql)  # replace multiple spaces to 1
    # print(f"after sql: {sql}")
    # "  france  " ---> "france"
    # 匹配以双引号包裹的内容，并且允许双引号内外出  返回所有匹配项   也就是 predicate value 但是非字符串的predicate value  是没有双引号的
    quotes = re.findall(r'" *.*? *"', sql)  
    for qt in quotes:
        new_qt = qt.strip('"') #去掉引号和空格
        new_qt = new_qt.strip()
        new_qt = '"' + new_qt + '"'
        sql = sql.replace(qt, new_qt)

    quotes = re.findall(r'" *.*? *"', sql) #为啥要重新再走一遍

    # print(f"sql:{sql}   quotes:{quotes}")
    print(f'before process sql: {sql}')

    for qt in quotes:
        new_qt = qt.strip('"')
        #两端 中间的空格 用特殊符号替换
        replaced_new_qt = 'a999specific999start999symbol' + new_qt
        replaced_new_qt = replaced_new_qt.replace(' ', '9999this0is0a0specific0hypen123654777')
        # replace symbols in the replace_dict
        for key in replace_dict.keys(): #一些 符号用特殊字符替换
            replaced_new_qt = replaced_new_qt.replace(key, replace_dict[key])
        replaced_new_qt = '\"' + replaced_new_qt + '\"' #把引号重新加上
        sql = sql.replace(qt, replaced_new_qt)


    # `` 反引号括起来的内容也要处理  因为里面可能会有空格之类的内容
    quotes_2 = re.findall(r'` *.*? *`', sql)  
    for qt in quotes_2:
        new_qt = qt.strip('`') #去掉引号和空格
        new_qt = new_qt.strip()
        new_qt = '`' + new_qt + '`'
        sql = sql.replace(qt, new_qt)

    quotes_2 = re.findall(r'` *.*? *`', sql) #为啥要重新再走一遍

    # print(f"sql:{sql}   quotes:{quotes}")
    for qt in quotes_2:
        new_qt = qt.strip('`')
        #两端 中间的空格 用特殊符号替换
        replaced_new_qt = 'a753275specific37575start37397symbol' + new_qt
        replaced_new_qt = replaced_new_qt.replace(' ', '786786his0is0a0sp7867867en1236783737')
        # replace symbols in the replace_dict
        for key in replace_dict.keys(): #一些 符号用特殊字符替换
            replaced_new_qt = replaced_new_qt.replace(key, replace_dict[key])
        replaced_new_qt = '`' + replaced_new_qt + '`' #把引号重新加上
        sql = sql.replace(qt, replaced_new_qt)

    print(f'after process sql: {sql}')
    # jzx TODO:  column / table name 也可能存在 空格  所以上面也需要对这些做处理   存在空格的table/column name一般用 ``包含
    temp_token_list = sql.split()  # to tokens  按照空格划分

    i = 0
    # sql中有用单引号括起来的内容 用随机生成的字符串代替 
    # print(f"temp_token_list:{temp_token_list}")
    while i < len(temp_token_list):
        if temp_token_list[i] == '\'':
            j = i + 1
            if j >= len(temp_token_list):
                print('exception: no matched here')
                break
            temp_str = ''
            while temp_token_list[j] != '\'': #单引号括起来的 放到 temp_str中 ---> jzx TODO: 不处理双引号? DONE 后面处理双引号了
                if i == j + 1:
                    temp_str += temp_token_list[j]
                else:
                    temp_str += ' ' + temp_token_list[j]
                j += 1
                if j >= len(temp_token_list):
                    print('exception: no matched here')
                    break
            temp_str = temp_str.strip(' ')
            # generate a random substituted value
            if temp_str not in g_dict.keys():
                temp_key = generateRandomLetterString()
                while True:
                    flag = 1
                    for temp_value in g_dict.values():
                        if temp_value.count(temp_key) > 0 or temp_key.count(temp_value): #子串出现的次数  也就是随机生成的字符串不能出现在g_dict中
                            flag = 0
                            break
                    if flag == 1:
                        break
                    else:
                        temp_key = generateRandomLetterString()
                g_dict[temp_str] = temp_key
            # delete the tokens from i to j ([i: j+1]) including ''
            del temp_token_list[i: j + 1]
            # replace this part with the new token
            temp_token_list.insert(i, g_dict[temp_str])
        # update i
        i += 1
    # "
    i = 0
    # sql中有用单引号括起来的内容 用随机生成的字符串代替
    while i < len(temp_token_list):
        if temp_token_list[i] == '\"':
            j = i + 1
            if j >= len(temp_token_list):
                print('exception: no matched here')
                break
            temp_str = ''
            while temp_token_list[j] != '\"':
                if i == j + 1:
                    temp_str += temp_token_list[j]
                else:
                    temp_str += ' ' + temp_token_list[j]
                j += 1
                if j >= len(temp_token_list):
                    print('exception: no matched here')
                    break
            temp_str = temp_str.strip(' ')
            # generate a random substituted value
            if temp_str not in g_dict.keys():
                temp_key = generateRandomLetterString()
                while True:
                    flag = 1
                    for temp_value in g_dict.values():
                        if temp_value.count(temp_key) > 0 or temp_key.count(temp_value):
                            flag = 0
                            break
                    if flag == 1:
                        break
                    else:
                        temp_key = generateRandomLetterString()
                g_dict[temp_str] = temp_key
            # delete the tokens from i to j ([i: j+1]) including ''
            del temp_token_list[i: j + 1]
            # replace this part with the new token
            temp_token_list.insert(i, g_dict[temp_str])
        # update i
        i += 1
    for i in range(len(temp_token_list)):
        # detect float number (with decimal point)
        if isNumber(temp_token_list[i]) and temp_token_list[i].count('.') > 0: #浮点数 把浮点数的. 变成特殊字符串
            temp_token_list[i] = temp_token_list[i].replace('.',
                                                            'numberfloatreplacingprocessthisisjustapaddingstringhaha')

    sql = ' '.join(temp_token_list)
    sql = sql.strip('; ') #去掉sql结尾的;
    print('--------')
    print(sql)
    print('--------')
    # exit(-1)
    #整个sql用 括号括起来 则去掉两端的括号
    while sql[0] == '(' and sql[-1] == ')':
        sql = sql.strip('()')
    sql = sql.strip('; ')

    #多个空格变成一个空格
    sql = re.sub(' +', ' ', sql)  # replace multiple spaces to 1
    res = sql
    #所有关键字大写
    res = capitalizeKeyword(res)  # capitalize keywords

    #去掉所有的单双引号
    res = res.replace('\'', '')
    res = res.replace('\"', '')

    res = re.sub(' +', ' ', res)  # replace multiple spaces to 1

    res = res.replace('(', ' ( ')
    res = res.replace(')', ' ) ')
    res = res.replace('<', ' < ')
    res = res.replace('>', ' > ')
    res = res.replace('=', ' = ')
    temp_tokens = res.split()
    res = ' '.join(temp_tokens)
    res = re.sub('! +=', '!=', res)
    res = re.sub('< +=', '<=', res)
    res = re.sub('> +=', '>=', res)
    res = re.sub(' +', ' ', res)  # replace multiple spaces to 1
    res = res.strip(' ')

    return res



def num2ordinalStr(num):
    ORDINAL_NUMBER = {1: "first", 2: "second", 3: "third", 4: "fourth", 5: "fifth", 6: "sixth", 7: "seventh",
                      8: "eighth", 9: "nineth", 10: "tenth", 11: "eleventh", 12: "twelfth", 13: "thirteenth",
                      14: "fourteenth", 15: "fifteenth", 16: "sixteenth", 17: "seventeenth", 18: "eighteenth",
                      19: "nineteenth"}
    if int(num) in ORDINAL_NUMBER.keys():
        res = ORDINAL_NUMBER[int(num)]
    else:
        res = str(num) + 'th'

    return res


def postprocess(explanation, ori_sql):
    global g_dict

    # substitute value with original value (especially for Name)

    # first, remove '', ""
    ori_sql = ori_sql.replace('\'', '')
    ori_sql = ori_sql.replace('\"', '')

    # tokenize explanation and sql
    tok_exp = explanation.split()
    tok_sql = ori_sql.split()

    for i in range(0, len(tok_exp)):
        for tk_sql in tok_sql:
            if tok_exp[i] == tk_sql.lower():
                tok_exp[i] = tk_sql

    # lower some special words (e.g. ' distinct ')

    result = ' '.join(tok_exp)
    result = result.replace(' DISTINCT ', ' distinct ')
    result = result.replace(' ON ', ' on ')
    result = result.replace(' BETWEEN ', ' between ')
    result = result.replace(' IN ', ' in ')
    result = result.replace(' NOT ', ' not ')
    # result = result.replace(' OF ', ' of ')
    result = result.replace(' THAT ', ' that ')
    result = result.replace(' HAS ', ' has ')

    # replace back for contents in '' or ""
    # result = result.replace(' hehehehehe ', ' and ')
    for key in g_dict.keys():
        temp_res = '''"''' + key + '''"'''
        result = result.replace(g_dict[key], temp_res)

    # replace back numberfloatreplacingprocessthisisjustapaddingstringhaha ---> .
    result = result.replace('numberfloatreplacingprocessthisisjustapaddingstringhaha', '.')

    result = re.sub(r' *, *', ', ', result)  # naturalize comma

    return result


def split_by_alias(group_clause):
    tok_group_clause = group_clause.split()
    for i in range(0, len(tok_group_clause)): 
        tok = tok_group_clause[i]
        if '.' in tok:
            temp_tok = tok.split('.')
            if len(temp_tok) != 2:
                print("exception: . should split it into 2 parts")
                break
            if temp_tok[0]+'.' in alias_maps_tb.keys():
                table_name = alias_maps_tb[temp_tok[0]+'.']
                table_name = table_name.split('.')[0]
            else:
                table_name = temp_tok[0]
            comma = ''
            if ',' in temp_tok[1]:
                comma = ','
                temp_tok[1] = temp_tok[1].replace(',', '')
            # Y of X
            res_str = '`'+temp_tok[1] +'`'+ ' of ' +'`'+ table_name +'`'+ comma
            tok_group_clause[i] = res_str
    group_clause = ''
    group_clause = ' '.join(tok_group_clause)
    return group_clause


'''
这个函数 把一个没有子查询的SQL根据关键字分成几部分
select xxx from xxx where xxx group by xxx order by xxx  这些关键词应该只能出现一次
'''
def parseSQL(sql): 
    global g_dict
    global replace_dict
    global g_schema # the database schema where the current SQL come from

    core_keywords = ['select', 'from', 'join', 'on', 'where', 'group by', 'having', 'order by', 'asc', 'desc']  # 'as' 去掉 as 可能只是别名

    # sql = sql.upper() # parser won't make the keyword upper in some cases. I need to make this for later checking
    p = Parser(sql)  # get the parser
    parsed = sqlparse.parse(sql)

    # list for subexpression
    sub_expression = []
    temp_subexpression = ''

    # list for natural language description
    explanations = []
    temp_explanation = ''

    '''
        get the subexpression and explanation for SELECT ... FROM ...(JOIN ... (AS) .... (ON) .... )
    '''
    # check if the first token is "SELECT"  跳到这个函数的sql  必须以select开头
    # jzx TODO: 下面这些是直接退出 还是返回呢
    if p.tokens[0].value.lower() != 'select':
        print("exception: the first word is not SELECT, it is " + p.tokens[0].value)
        raise ValueError("exception: the first word is not SELECT, it is " + p.tokens[0].value)
        # exit(-1)
        return sub_expression, explanations
    # sql中只能出现一个select  并且只能在开头处
    if sql.lower().count('select') > 1:
        print(f"exception: more than two select clause. sql:{sql}")
        raise ValueError(f"exception: more than two select clause. sql:{sql}")
        # exit(-1)
        return sub_expression, explanations
    # error check: there must exists FROM in sql    跳到这个函数的sql  必须包含from
    if ' from ' not in sql.lower():
        print("exception: no FROM")
        print(f"sql: {sql}")
        raise ValueError("exception: no FROM")
        # exit(-1)
        # return sub_expression, explanations

    temp_tokens = [] # select xxx from xxx 这些从sql中提取出来
    temp_flag = 0  # denote if "FROM" or "JOIN" is encountered
    range_idx = 0
    # match the substring before the next keyword of "FROM"    如果join不存在在sql中
    if ' JOIN ' not in sql and ' join ' not in sql:
        for tok in p.tokens:
            if tok.value == "FROM" or tok.value == "from":
                temp_flag = 1
            if temp_flag == 1 and tok.value != 'FROM' and tok.value != 'from' and tok.value.lower() in core_keywords:
                break
            temp_tokens.append(tok.value) #把from 之后下一个keywords 出现之前的内容存储起来
    else:
        # first, find the last index of 'AS' or 'ON'
        for i in range(0, len(p.tokens)):  #最后一个 join/as/on 出现的位置    这里的as 不太对  无法确定就是 join分割的地方
            #jzx TODO:  or p.tokens[i].value.lower() == 'as' \ 去掉这些as 的判断 假定 多表join的格式 是  join xxx as t1 on t1.aa=t2.bb  最后是根据on 来结束多表join的，所以as不是很重要，而且as 容易引起歧义 因为sql的其他地方也会出现 别名设置
            if p.tokens[i].value.lower() == 'join' \
                    or p.tokens[i].value.lower() == 'on' or p.tokens[i].value.lower() == 'inner join' or p.tokens[i].value.lower() == 'cross join':
                # join xxx as t1 on
                range_idx = i

        # second, find the index of next key word
        temp_flag = False # used to indicate if the next keyword is met
        # join/as/on 之后下一个keywords 出现的位置
        for i in range(range_idx + 1, len(p.tokens)):
            if p.tokens[i].value.lower() in core_keywords:  # core_keywords中去掉 as  别名应该包括在 select  xxx from xxx中
                temp_flag = True
                range_idx = i  # the range is from 0 to the position before next keyword
                break
        # if next keyword hasn't been met, it means it mets EOF,
        if not temp_flag:
            range_idx = len(p.tokens)

        # get the subexpression
        for _ in range(0, range_idx):  #where 之前的
            temp_tokens.append(p.tokens[_].value) 
    temp_subexpression = ' '.join(temp_tokens) #temp_tokens存的是  select xxx from xxx 的内容
    sub_expression.append(temp_subexpression)


    p1 = Parser(temp_subexpression)  # get the parser for subexpression  要是有别名  这么分是不是有问题了
    # generate explanation for 'FROM ... [JOIN <nouns> [ON <Condition>]]'
    from_tbs = []  # used to store the table name of FROM    存储from  和 join 之后的table name --->有别名咋办?
    join_tbs = []  # used to store the table name of JOIN
    i = 0
    # for i in range(0, len(p1.tokens)):
    from_index = -1
    while i < len(p1.tokens): #p1 是where 之前的   where之后的内容不用担心
        if p1.tokens[i].value == 'FROM' or p1.tokens[i].value == 'from': #from 之后的, 逗号才行
            from_index = i
            # from_tb = p1.tokens[i].next_token.value  # FROM tb1, tb2 这样的形式也是符合语法的
            from_tbs.append(p1.tokens[i].next_token.value)
        elif p1.tokens[i].value == ',' and from_index > 0 and i > from_index:
            from_tbs.append(p1.tokens[i].next_token.value)
        elif p1.tokens[i].value.lower() == 'join' or p1.tokens[i].value.lower() == 'inner join':
            join_tbs.append(p1.tokens[i].next_token.value)
        elif p1.tokens[i].value.lower() == 'as' and (p1.tokens[i].previous_token.value in from_tbs or p1.tokens[i].previous_token.value in join_tbs): #记录table 别名和全称的对应关系 -->这里只记录table的别名  但是sql中别名不只存在在table中  
            #as 前面必须是table name   FROM tb as xxx  /  FROM tb as xxx JOIN tb2 as yyy ON xxx.a=yyy.b 
            alias_tb = p1.tokens[i].next_token.value+'.' #别名加.  这样才独一无二
            if alias_tb not in alias_maps_tb.keys():
                alias_maps_tb[alias_tb] = p1.tokens[i].previous_token.value+'.'
            elif alias_maps_tb[alias_tb] != p1.tokens[i].previous_token.value:
                print(f"alias setting table error. alias_tb:{alias_tb}; alias_maps[alias_tb]:{alias_maps_tb[alias_tb]}; value_setting:{p1.tokens[i].previous_token.value}")
        i += 1
    # remove repetitive elements (while keep the order unchanged)
    join_tbs_set = []  #去掉重复的table
    for j in range(len(join_tbs)):
        if join_tbs[j] not in join_tbs_set:
            join_tbs_set.append(join_tbs[j])
    from_tbs_set = []
    for j in range(len(from_tbs)):
        if from_tbs[j] not in from_tbs_set:
            from_tbs_set.append(from_tbs[j])
    from_exp = ' from TABLE ' + '`'+from_tbs_set[0]+'`'
    for j in range(1, len(from_tbs_set)):
        if j == len(from_tbs_set) - 1:
            from_exp += ', and TABLE ' + '`'+from_tbs_set[j]+'`'
        else:
            from_exp += ', TABLE ' + '`'+from_tbs_set[j]+'`'
    for j in range(0, len(join_tbs_set)):
        if j == len(join_tbs_set) - 1:
            from_exp += ', and TABLE ' + '`'+join_tbs_set[j]+'`'
        else:
            from_exp += ', TABLE ' + '`'+join_tbs_set[j]+'`'

    # randomly choose a verb
    # verb = ['Find out', 'Find', 'Get', 'Show me', 'Show', 'List', 'Give me', 'Tell me', 'search']
    verb = ['Return']  #可以认为是模板

    #jzx TODO: 截取 select  和 from 之间的内容  根据from_index 定位 
    select_tokens = []
    for i in range(1, len(p1.tokens)):
        if i < from_index: #from_index记录 from出现的索引
            select_tokens.append(p1.tokens[i].value)
    select_subexpression = ' '.join(select_tokens)
    select_nl = getNouns(select_subexpression, sql)
    temp_explanation = random.choice(verb)  #verb是返回的模板 --->可以就是一种，也可以是多种:  这里固定为Return
    temp_explanation += select_nl

    # instead just delete [alias]
    temp_explanation_00 = temp_explanation.replace(',', ' , ')
    exp_token = temp_explanation_00.split()
    for i in range(0, len(exp_token)):
        tok = exp_token[i]
        if '.' in tok:
            temp_tok = tok.split('.')
            if len(temp_tok) != 2:
                print("exception: . should split it into 2 parts")
                break
            exp_token[i] = '`'+temp_tok[1]+'`' + ' of ' +'`' +temp_tok[0]+'`'  #jzx TODO: 返回有别名咋办, 是用别名  还是原始表名? Done 用原始表 应该不会出现这种情况了

    temp_explanation = ' '.join(exp_token)

    temp_explanation += from_exp  # add explanation for ''

    explanations.append(temp_explanation)

    '''
        get the subexpression and explanation for "WHERE ..."
    '''

    # if ' WHERE ' in sql or ' where ' in sql:   接下来处理 where
    # xyr
    if len( [token for token in parsed[0] if isinstance(token, sqlparse.sql.Where)]) !=0: #如果sql中存在where
        # get the subexpression for where clause
        where_clause = [token for token in parsed[0] if isinstance(token, sqlparse.sql.Where)][0] #where 后面所有的内容?

        # get the explanation for where clause
        sub_expression.append(str(where_clause))  #存的是 拆开的子句

        # replace operators with natural language
        str_where_clause = str(where_clause)

        if str_where_clause.lower().count('where') > 1:
            print(f"exception: more than two where clause. sql:{str_where_clause}")
            raise ValueError(f"exception: more than two where clause. sql:{str_where_clause}")
            # exit(-1)
        
        str_where_clause = split_by_alias(copy.deepcopy(str_where_clause))
        temp_explanation = ("Keep the records " + str_where_clause)
        temp_explanation = temp_explanation.strip(' ')

        explanations.append(temp_explanation)

    '''
        get the subexpression and explanation for "GROUP BY ... (HAVING ...)"
    '''

    if 'group by' in sql.lower():  #对sql中的group by / order by / having count等做处理
        if sql.lower().count('group by') > 1:
            print(f"exception: more than two group by clause. sql:{sql}")
            raise ValueError(f"exception: more than two group by clause. sql:{sql}")
            # exit(-1)
        temp_explanation = ''
        if 'GROUP BY' in sql:
            group_clause = getSubExpressionBeforeNextKeyword(sql, 'GROUP BY')
        else:
            group_clause = getSubExpressionBeforeNextKeyword(sql, 'group by')

        noun = getNouns(group_clause, sql) #group by的column 转成NL

        temp_explanation = 'Group the records based on ' + noun #group by以及对应的column
        # replace '*' with "it"
        if '*' in noun:
            noun = noun.replace('*', 'it')

        temp_subexpression = group_clause

        # having
        # there may be 'OR' or 'AND' so

        if ' HAVING ' in sql or ' having ' in sql:
            p = Parser(sql)  # get the parser
            temp_flag = 0  # denote if the keyword is encountered
            have_clause = getSubExpressionBeforeNextKeyword(sql, 'HAVING')
            temp_subexpression += ' ' + have_clause

            have_clause = split_by_alias(copy.deepcopy(have_clause))
            temp_explanation += (' where ' + have_clause)  #这里的having 应该特殊处理  如果是having count(xxx) 应该把count/the number of这个意思翻译出来

        explanations.append(temp_explanation)

        sub_expression.append(temp_subexpression)

    '''
        get the subexpression and explanation for "ORDER BY ... (ASC/DESC) (LIMIT (value))"
    '''
    if 'order by' in sql.lower():
        if sql.lower().count('order by') > 1:
            print(f"exception: more than two order by clause. sql:{sql}")
            raise ValueError(f"exception: more than two order by clause. sql:{sql}")
            # exit(-1)
        temp_explanation = ''
        if 'ORDER BY' in sql:
            order_clause = getSubExpressionBeforeNextKeyword(sql, 'ORDER BY')
        else:
            order_clause = getSubExpressionBeforeNextKeyword(sql, 'order by')

        noun = getNouns(order_clause, sql)
        # temp_explanation = 'Order these records based on ' + noun
        temp_explanation = 'Sort the records in '

        sorting = ' '  # used to express ASC or DESC
        limit_clause = ''
        if ' asc' in sql.lower() or ' desc' in sql.lower() or ' limit' in sql.lower():  #jzx TODO: 这里直接翻译成 ascending/descending order好么? 还是说翻译成 highest / lowest 这些词比较好呢?
            if ' asc' in sql.lower():
                # temp_explanation += ' and sort them in ascending order'
                temp_explanation += 'ascending order'
                sorting = ' ASC'
            elif ' desc' in sql.lower():
                # temp_explanation += ' and sort them in descending order'
                temp_explanation += 'descending order'
                sorting = ' DESC'
            # default: asc
            else:
                # temp_explanation += ' and sort them in ascending order'
                temp_explanation += 'ascending order'
                sorting = ' '
        # default: asc
        else:
            # temp_explanation += ' and sort them in ascending order'
            temp_explanation += 'ascending order'
            sorting = ' '

        temp_explanation += ' based on ' + noun

        if ' limit' in sql.lower():
            if ' LIMIT' in sql:
                limit_clause = ' ' + getSubExpressionBeforeNextKeyword(sql, 'LIMIT')
            else:
                limit_clause = ' ' + getSubExpressionBeforeNextKeyword(sql, 'limit')
            for tok in p.tokens:
                if tok.value == 'LIMIT' or tok.value == 'limit':
                    if tok.next_token.position != -1:
                        if not tok.next_token.is_integer:
                            print("exception: the limit value should be an integer")
                        limit_value = tok.next_token.value
                        if int(limit_value) == 1:
                            temp_explanation += ', and return the first record'
                        elif int(limit_value) > 1:
                            temp_explanation += ', and return the top ' + limit_value + ' records'
                    else:
                        temp_explanation += ', and return the first record'

        order_clause = order_clause + sorting + limit_clause
        sub_expression.append(order_clause)

        explanations.append(temp_explanation)

    # add \" to all quoted values
    # replace '_' with ' ' in explanations
    # postprocess explanations   explanations 解释分成4个方面进行解释 : 1) FROM table join （多表只是提了 table name 没有说join column）  2) WHERE selection condition  3) group by  4) order by
    for i in range(0, len(explanations)):
        # add '\"' to explanations
        temp_tok_list = explanations[i].split()
        for k in range(len(temp_tok_list)): #之前引号包含的一些 特殊字符串做处理
            if 'a999specific999start999symbol' in temp_tok_list[k]:
                temp_tok_list[k] = '\"' + temp_tok_list[k] + '\"' # add '\"' if it contains the hypen

            if 'a753275specific37575start37397symbol' in temp_tok_list[k]:
                temp_tok_list[k] = '`' + temp_tok_list[k] + '`' # add '\"' if it contains the hypen
        # update explanation
        explanations[i] = ' '.join(temp_tok_list)

        explanations[i] = postprocess(explanations[i], sql) #全部变成小写字母
        explanations[i] = explanations[i].replace('9999this0is0a0specific0hypen123654777', ' ')
        explanations[i] = explanations[i].replace('a999specific999start999symbol', '') #把特殊字符去掉

        explanations[i] = explanations[i].replace('786786his0is0a0sp7867867en1236783737', ' ')
        explanations[i] = explanations[i].replace('a753275specific37575start37397symbol', '') #把特殊字符去掉

        # add '\"' to subexpressions
        temp_tok_list = sub_expression[i].split() #子 SQL
        for k in range(len(temp_tok_list)):
            if 'a999specific999start999symbol' in temp_tok_list[k]:
                temp_tok_list[k] = '\"' + temp_tok_list[k] + '\"' # add '\"' if it contains the hypen
            if 'a753275specific37575start37397symbol' in temp_tok_list[k]:
                temp_tok_list[k] = '`' + temp_tok_list[k] + '`' # add '\"' if it contains the hypen
        # update explanation
        sub_expression[i] = ' '.join(temp_tok_list)

        # replace back for decimal point: numberfloatreplacingprocessthisisjustapaddingstringhaha ---> .
        sub_expression[i] = sub_expression[i].replace('numberfloatreplacingprocessthisisjustapaddingstringhaha', '.')
        sub_expression[i] = sub_expression[i].replace('9999this0is0a0specific0hypen123654777', ' ')
        sub_expression[i] = sub_expression[i].replace('a999specific999start999symbol', '')

        sub_expression[i] = sub_expression[i].replace('786786his0is0a0sp7867867en1236783737', ' ')
        sub_expression[i] = sub_expression[i].replace('a753275specific37575start37397symbol', '') #把特殊字符去掉


        for key in replace_dict.keys(): #把特殊字符 从sql 和 explanation中都去掉 
            explanations[i] = explanations[i].replace(replace_dict[key], key)
            sub_expression[i] = sub_expression[i].replace(replace_dict[key], key)
        for key in g_dict.keys():
            temp_res = '''"''' + key + '''"'''
            sub_expression[i] = sub_expression[i].replace(g_dict[key], temp_res)

        # if there exists a natural name dict (spider), use it
        # replace both column and table name
        explanations[i] = re.sub(r' *, *', ' , ', explanations[i]) # separate comma

        temp_exp_tok_list = explanations[i].split() # get explanation token list

        if natural_name_flag and g_schema: 
            for tok_idx in range(len(temp_exp_tok_list)):  #original name 和表格中的name不一致时  这里最好不变 以便和 schema对应上  --->jzx TODO: 两种形式都保留也可以
                # replace column names
                # column_names_original 是存在数据库中的
                # replace table names
                for idx_table in range(len(g_schema['table_names_original'])):
                    if temp_exp_tok_list[tok_idx].lower() == g_schema['table_names_original'][idx_table].lower() and (g_schema['table_names_original'][idx_table].lower() != g_schema['table_names'][idx_table].lower()):
                        temp_exp_tok_list[tok_idx] = temp_exp_tok_list[tok_idx] + ' ( ' + g_schema['table_names'][idx_table] + ' ) '

                    for idx_column in range(1, len(g_schema['column_names_original'])):
                        if g_schema['column_names_original'][idx_column][0] == idx_table and temp_exp_tok_list[tok_idx].lower() == g_schema['column_names_original'][idx_column][1].lower() and (g_schema['column_names_original'][idx_column][1].lower() != g_schema['column_names'][idx_column][1].lower()):
                            temp_exp_tok_list[tok_idx] = temp_exp_tok_list[tok_idx] + ' ( ' + g_schema['column_names'][idx_column][1] + ' ) '
            # token list to string
            explanations[i] = ' '.join(temp_exp_tok_list)

            # lower keywords
            explanations[i] = explanations[i].replace(' FROM ', ' from ')
            explanations[i] = explanations[i].replace(' ORDER ', ' order ')
            explanations[i] = explanations[i].replace(' OR ', ' or ')
            explanations[i] = explanations[i].replace(' AND ', ' and ')
            explanations[i] = explanations[i].replace(' WHERE ', ' where ')
            explanations[i] = explanations[i].replace(' HAVING ', ' having ')
            # otherwise, just remove '_' of nouns
        else:
            pass #不要去掉 column name中的 下划线
            # explanations[i] = explanations[i].replace('_', ' ')

        explanations[i] = re.sub(r' *, *', ', ', explanations[i])  # naturalize comma
    print([sub_expression, explanations])  #explanation 包括自然语言解释 和其对应的子sql
    return [sub_expression, explanations]



def simpleCompose(subs):
    res = ''
    categories = ['select', 'from', 'where', 'group', 'having', 'order']
    # 6 categories
    subexpression_dict = {
        # 'select': '',
        # 'from': '',
        # 'where': '',
        # 'group': '',
        # 'having': '',
        # 'order': '',
    }
    for sub in subs:
        if sub.lower().startswith('select ') and 'select' not in subexpression_dict.keys():
            subexpression_dict['select'] = sub
        elif sub.lower().startswith('from ') and 'from' not in subexpression_dict.keys():
            subexpression_dict['from'] = sub
        elif sub.lower().startswith('where ') and 'where' not in subexpression_dict.keys():
            subexpression_dict['where'] = sub
        elif sub.lower().startswith('group by ') and 'group' not in subexpression_dict.keys():
            subexpression_dict['group'] = sub
        elif sub.lower().startswith('having ') and 'having' not in subexpression_dict.keys():
            subexpression_dict['having'] = sub
        elif sub.lower().startswith('order by ') and 'order' not in subexpression_dict.keys():
            subexpression_dict['order'] = sub

    # handle missing select
    if 'select' not in subexpression_dict.keys():
        subexpression_dict['select'] = 'SELECT *'

    for key in categories:
        if key in subexpression_dict.keys():
            res += ' ' + subexpression_dict[key]

    res = res.strip()

    return res


def sql2nl(sql, one_db_schema):
    global high_level_explanation
    global g_p
    global alias_maps_tb
    global g_schema
    g_schema = one_db_schema

    high_level_explanation = [] #每次都要清空 list
    alias_maps_tb = dict()

    total_high_level_explanation = dict()  #对应 多个with 子句的

    # sql = preprocessSQL(sql)
    # parsed = sqlparse.parse(sql) #这个没啥用?
    # global g_p
    # sql = 'SELECT a FROM tb1 AS T1 JOIN tb2 AS T2 ON T1.id = T2.id  WHERE T1.a > (SELECT AVG(b) FROM tb3 WHERE b > 10)'
    # sql = 'SELECT * FROM sub1, sub2'
    g_p = Parser(sql)  # get the parser
    # construct high level explanation
    # for test
    # 先解析 g_p 中是否存在 with
    with_table2queries = {}
    with_tables = []
    multiple_subquery_error_list = {}
    try:
        with_name_len = len(g_p.with_names)
    except Exception as e:
        print('testttttttttt-1')
        print(f"e:{e}")
        print('testttttttttt 0')
        multiple_subquery_error_list['error'] = sql
        return total_high_level_explanation
    final_sub_sql = copy.deepcopy(g_p.query)
    
    #左括号前后的空格去掉  右括号前面的空格去掉
    if with_name_len > 0: #说明sql中存在 with 子句
        print(f"with tables:{g_p.with_names}")
        for i in range(with_name_len):
            with_tables.append(g_p.with_names[i])
        for one_table in with_tables:
            with_table2queries[one_table] = g_p.with_queries[one_table]
            sub_sql = str(g_p.with_queries[one_table])
            alias_maps_tb = dict() #每个with 子句只用重置 table的别名 因为 function的别名在别的with 子句可能会用到
            high_level_explanation = []
            print(f"sub_sql:{sub_sql}")
            sub_sql = preprocessSQL(sub_sql)
            #第二个子sql有问题 sub_sql
            ret = decompose(sub_sql)  #jzx TODO: join 多表的 解释 是否可以优化  直接 table xxx and table xxx 是否合理?
            if ret == 'multiple sub-queries, return false':
                if 'cannot_parse_subquery' not in multiple_subquery_error_list.keys():
                    multiple_subquery_error_list['cannot_parse_subquery'] = {}
                multiple_subquery_error_list['cannot_parse_subquery'][one_table] = sub_sql
            # 这里处理 每个子句让LLM 生成
            reorganize_explanations()
            total_high_level_explanation[one_table] = copy.deepcopy(high_level_explanation[:])

        # 去掉 final_sub_sql 中的WITH 子句
        if "WITH" in final_sub_sql.upper():
            # Use regex to remove the WITH clause
            # This regex will capture the part after the last closing parenthesis of WITH clause
            modified_query = re.sub(r"WITH\s+.*?\)\s*(SELECT)", r"\1", final_sub_sql, flags=re.IGNORECASE | re.DOTALL)
        else:
            modified_query = final_sub_sql

        # if modified_query == 'SELECT T1.account_id, ( CAST ( ( SELECT a11 FROM highest_avg_salary ) AS REAL ) - CAST ( ( SELECT a11 FROM lowest_avg_salary ) AS REAL ) ) AS salary_gap FROM account AS T1 WHERE T1.district_id IN ( SELECT district_id FROM oldest_female )':
        #     stop_it = 0
        alias_maps_tb = dict()
        high_level_explanation = []
        print(f"modified_query:{modified_query}")
        print(f"original query:{final_sub_sql}")
        ret = decompose(modified_query)  #jzx TODO: join 多表的 解释 是否可以优化  直接 table xxx and table xxx 是否合理?
        if ret == 'multiple sub-queries, return false':
            if 'cannot_parse_subquery' not in multiple_subquery_error_list.keys():
                multiple_subquery_error_list['cannot_parse_subquery'] = {}
            multiple_subquery_error_list['cannot_parse_subquery']['final_sql'] = modified_query
        reorganize_explanations()
        print(f"high_level_explanation final:\n{high_level_explanation}")
        total_high_level_explanation['final'] = copy.deepcopy(high_level_explanation[:])
        

    else:
        sql = preprocessSQL(sql)
        ret = decompose(sql)  #jzx TODO: join 多表的 解释 是否可以优化  直接 table xxx and table xxx 是否合理?
        if ret == 'multiple sub-queries, return false':
            if 'cannot_parse_subquery' not in multiple_subquery_error_list.keys():
                multiple_subquery_error_list['cannot_parse_subquery'] = {}
            multiple_subquery_error_list['cannot_parse_subquery']['final_sql'] = sql
        reorganize_explanations()
        total_high_level_explanation['all'] = copy.deepcopy(high_level_explanation[:])

    
    # update subqueries in the datastructure (make sure there is no the123first123query ....)
    for subSQL, one_high_level_explanation in total_high_level_explanation.items():
        print(f"sub sql : {subSQL}")
        for idx, unit in enumerate(one_high_level_explanation):
            # print(f"one_high_level_explanation:\n{one_high_level_explanation}")
            if not isinstance(unit['explanation'], str):
                temp_subquery_str = simpleCompose(unit['explanation'][0]) #['explanation'][0]是 sub SQL --->合成最终的sql -->为啥要合成之前不是本来就有 original sql吗
                one_high_level_explanation[idx]['subquery'] = temp_subquery_str
            # print(f"simpleCompose temp_subquery_str:{temp_subquery_str};  \nunit['explanation'][0]:{unit['explanation'][0]}")

        # the first query result -> start the first query,
        for i in range(len(one_high_level_explanation)):
            one_high_level_explanation[i]['number'] = 'Start ' + one_high_level_explanation[i]['number'] + ','
            one_high_level_explanation[i]['number'] = one_high_level_explanation[i]['number'].replace('query result', 'query')

            # modify pure explanation for 'xxx INT yyy'
            if isinstance(one_high_level_explanation[i]['explanation'], str):
                temp1 = [[one_high_level_explanation[i]['subquery']], [one_high_level_explanation[i]['explanation']], [one_high_level_explanation[i]['explanation']]]
                one_high_level_explanation[i]['explanation'] = temp1

            # explanation[[subexpression], [explanation]] --->explanation[{subexpression, explanation}, ...]
            newList = []

            for j in range(len(one_high_level_explanation[i]['explanation'][0])):
                # print(f"one_high_level_explanation[i]['explanation']:\n{one_high_level_explanation[i]['explanation']}")
                temp = {
                    'subexpression': one_high_level_explanation[i]['explanation'][0][j],
                    'explanation': one_high_level_explanation[i]['explanation'][1][j],
                    'llm_explanation': one_high_level_explanation[i]['explanation'][2][j]
                }

                newList.append(temp)

            one_high_level_explanation[i]['explanation'] = newList
    if len(multiple_subquery_error_list) > 0:
        total_high_level_explanation['error'] = multiple_subquery_error_list
    return total_high_level_explanation
