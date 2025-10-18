import json
input_path = "KaggleDBQA/classified/GeoNuclearData/extra.json"
with open(input_path, 'r') as f:
    datas = json.load(f)

gold = []
for data in datas:
    gold.append(data['query'])

output_path = "./gold.txt"
with open(output_path, 'w') as f:
    for i in gold:
        f.write(i+"\n")
