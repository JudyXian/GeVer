import json
import os
import sys
dir_b_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'util'))
sys.path.append(dir_b_path)
from transform_style_for_generate_sql import *
from get_schema import get_filtered_schemas_with_description
from decomposition_template import *
from composition_template import *
import argparse




def decomposition(all_path, output_decomp_path, sl_path, is_chess, output_decomp_path_new2="", decomposition_cnt=1, model='deepseek-chat'):
    
    with open(all_path, 'r') as all_f:
        all_data = json.load(all_f)
    
    with open(sl_path, 'r') as sl_f:
        linking_infos = json.load(sl_f) 

    all_similar_values_all = []
    with open('all_similar_values_for_bird.json','r') as f_similar:  
        for line in f_similar.readlines():
            if line.strip():
                all_similar_values_all.append(json.loads(line)) 

    link_index = 0
    linking_infos_index = 0
    decomposition_num = 0

    decomposed_data = None
    if output_decomp_path_new2 != "":
        with open(output_decomp_path_new2, 'r') as decomp_new2_f:
            decomposed_data = json.load(decomp_new2_f)

    for one_data in all_data:
        difficulty = one_data['difficulty']
        question = one_data['question']
        evidence = one_data['evidence']
        db_id = one_data['db_id']
        question_index = one_data['question_id']
        if difficulty == 'challenging':  #只考虑challenging的数据
            linking_infos_index = 95
            filter_similar_values, schemas, _, _, _, filtered_tables = get_filtered_schemas_with_description(question, all_similar_values_all[question_index], linking_infos[linking_infos_index], db_id, is_chess)
            decomposition_ans = []
            composition_ans = []
            linking_infos_index+=1
            for decomp_index in range(decomposition_cnt):
                # TODO: 如何进行
                if output_decomp_path_new2 != "": #复用分解的结果
                    tmp_decomposition_ans = decomposed_data[question_index]['decomposition_ans'][decomp_index]
                    print(f"test linking_infos_index:{linking_infos_index}")
                else:
                    tmp_decomposition_ans = decomposition_prompt(question,evidence, schemas, model=model) #需要分解 -->分解后的结果
                decomposition_ans.append(tmp_decomposition_ans)
                print(f"tmp_decomposition_ans:{tmp_decomposition_ans}")

                indep_ans = tmp_decomposition_ans['indep_ans']
                select_ans = tmp_decomposition_ans['select_ans']
                from_ans = tmp_decomposition_ans['from_ans']
                where_ans = tmp_decomposition_ans['where_ans']

                composition_input_cost = 0
                composition_output_cost = 0
                
                indep_ans_sql = ""
                if  "no decomposition" not in indep_ans.lower().strip(): #没有被分解
                    indep_ans_sql, tmp_input_cost, tmp_output_cost = composition_prompt(question,evidence, schemas, indep_ans, "indep", filtered_tables,model=model)
                    composition_input_cost += tmp_input_cost
                    composition_output_cost += tmp_output_cost
                # TODO: post_processing_for_composition

                select_ans_sql = ""
                if "no decomposition" not in select_ans.lower().strip():
                    select_ans_sql, tmp_input_cost, tmp_output_cost = composition_prompt(question,evidence, schemas, select_ans, "select", filtered_tables,model=model)
                    composition_input_cost += tmp_input_cost
                    composition_output_cost += tmp_output_cost

                from_ans_sql = ""
                if "no decomposition" not in from_ans.lower().strip():
                    from_ans_sql, tmp_input_cost, tmp_output_cost = composition_prompt(question,evidence, schemas, from_ans, "from", filtered_tables,model=model)
                    composition_input_cost += tmp_input_cost
                    composition_output_cost += tmp_output_cost

                where_ans_sql = ""
                if "no decomposition" not in where_ans.lower().strip():
                    where_ans_sql, tmp_input_cost, tmp_output_cost = composition_prompt(question,evidence, schemas, where_ans, "where", filtered_tables,model=model)
                    composition_input_cost += tmp_input_cost
                    composition_output_cost += tmp_output_cost

                print(f"indep_ans_sql:{indep_ans_sql}")
                print(f"select_ans_sql:{select_ans_sql}")
                print(f"from_ans_sql:{from_ans_sql}")
                print(f"where_ans_sql:{where_ans_sql}")

                tmp_composition_ans = {
                    "indep_ans_sql":indep_ans_sql,
                    "select_ans_sql":select_ans_sql,
                    "from_ans_sql":from_ans_sql,
                    "where_ans_sql":where_ans_sql,
                    "composition_input_cost":composition_input_cost,
                    "composition_output_cost":composition_output_cost
                }
                composition_ans.append(tmp_composition_ans)
            one_data['decomposition_ans'] = decomposition_ans
            one_data['composition_ans'] = composition_ans
            decomposition_num+=1
        link_index += 1

    print(f"decomposition_num:{decomposition_num}")
    with open(output_decomp_path, 'w') as outpu_f:
        json.dump(all_data, outpu_f, indent=4)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--decompose_name", type=str, default="decompose_03_30")
    parser.add_argument("--model_name", type=str, default="deepseek-chat")
    args = parser.parse_args()
    
    decompose_name = args.decompose_name
    MODEL =args.model_name
    
    print('decompose_name:'+str(decompose_name))
    print('model:'+str(MODEL))
    
    
    prefix_path = 'results/decomposition/' + decompose_name
    all_dev_path = 'all_dev.json'
    
    sl_path = 'res.json'  #schema linking 文件  
    

    output_decomp_path_new = prefix_path+'/add_decomposition_all_res_chess_'+MODEL+'.json'  #分解后的结果  

    is_chess = True # is_chess=true 就用chess预测的schema结果  否则就用真实的schema??
    decomposition_cnt = 1 # 一个问题同一个分解模板调用的次数 --->为了减少幻觉??
    output_decomp_path_new2 = ''
    
    decomposition(all_dev_path, output_decomp_path_new, sl_path, is_chess, output_decomp_path_new2 = output_decomp_path_new2, decomposition_cnt=decomposition_cnt, model=MODEL)
    