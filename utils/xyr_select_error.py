import json

root_path = "output/2.28_v2/"
input_path = root_path+"8_final.json"
output_path = root_path+"error.json"
with open(input_path, 'r') as f:
    datas = json.load(f)

error = []
for index, data in enumerate(datas):
    flag = 0
    for i in data['diff_exec']:
        if list(i.values())[0] == 1:
            flag = 1
            break
    if flag == 0:
        data['index'] = index
        error.append(data)
with open(output_path, 'w') as f:
    json.dump(error, f)

# # dail2
# nodecomposition = [1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 1, 1, 1, 0,
#                    1, 0, 1, 1, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1]
# # dail1
# nodecomposition = [1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 1, 0, 1, 1, 1, 0, 1, 0, 0, 1, 1, 1, 0,
#                    1, 0, 1, 1, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1, 0, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1]
# error_correct = []
# error_error = []
# for index, i in enumerate(error):
#     if nodecomposition[index] == 1:
#         error_correct.append(i)
#     else:
#         error_error.append(i)
# print(len(error_correct))
# print(error_correct)
# print(len(error_error))
# print(error_error)
# score = 0
# for i in nodecomposition:
#     score += i
# score = score/len(nodecomposition)
# print(score)


# from_path = "1_briged_from_decomposition_gpt3.5.json"
# not_path = "1_briged_not_decomposition_gpt3.5.json"
# where_path = "1_briged_where_decomposition_gpt3.5.json"
# combination_path = "1_combination_decomposition_gpt3.5.json"
# from_out = root_path+"error/"+from_path
# not_out = root_path+"error/"+not_path
# where_out = root_path+"error/"+where_path
# combination_out = root_path + "error/"+combination_path
# from_path = root_path+"1_briged_from_decomposition_gpt3.5.json"
# not_path = root_path+"1_briged_not_decomposition_gpt3.5.json"
# where_path = root_path+"1_briged_where_decomposition_gpt3.5.json"
# combination_path = root_path+"1_combination_decomposition_gpt3.5.json"
# from_data = []
# not_data = []
# where_data = []
# combination_data = []

# with open(from_path, 'r') as f:
#     datas = json.load(f)
# for index, i in enumerate(datas):
#     if index in error:
#         from_data.append(i)

# with open(not_path, 'r') as f:
#     datas = json.load(f)
# for index, i in enumerate(datas):
#     if index in error:
#         not_data.append(i)

# with open(where_path, 'r') as f:
#     datas = json.load(f)
# for index, i in enumerate(datas):
#     if index in error:
#         where_data.append(i)

# with open(combination_path, 'r') as f:
#     datas = json.load(f)
# for index, i in enumerate(datas):
#     if index in error:
#         combination_data.append(i)

# with open(from_out, 'w') as f:
#     json.dump(from_data, f)
# with open(not_out, 'w') as f:
#     json.dump(not_data, f)
# with open(where_out, 'w') as f:
#     json.dump(where_data, f)
# with open(combination_out, 'w') as f:
#     json.dump(combination_data, f)
