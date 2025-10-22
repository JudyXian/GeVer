import json


def get_db_schemas(all_db_infos, db_name):
    db_schemas = {}

    for db in all_db_infos:
        table_names_original = db["table_names_original"]
        table_names = db["table_names"]
        column_names_original = db["column_names_original"]
        column_names = db["column_names"]
        column_types = db["column_types"]

        db_schemas[db["db_id"]] = {}

        primary_keys, foreign_keys = [], []
        # record primary keys
        for pk_column_idx in db["primary_keys"]:
            pk_table_name_original = table_names_original[column_names_original[pk_column_idx][0]]
            pk_column_name_original = column_names_original[pk_column_idx][1]

            primary_keys.append(
                {
                    "table_name_original": pk_table_name_original.lower(),
                    "column_name_original": pk_column_name_original.lower()
                }
            )

        db_schemas[db["db_id"]]["pk"] = primary_keys

        # record foreign keys
        for source_column_idx, target_column_idx in db["foreign_keys"]:
            fk_source_table_name_original = table_names_original[
                column_names_original[source_column_idx][0]]
            fk_source_column_name_original = column_names_original[source_column_idx][1]

            fk_target_table_name_original = table_names_original[
                column_names_original[target_column_idx][0]]
            fk_target_column_name_original = column_names_original[target_column_idx][1]

            foreign_keys.append(
                {
                    "source_table_name_original": fk_source_table_name_original.lower(),
                    "source_column_name_original": fk_source_column_name_original.lower(),
                    "target_table_name_original": fk_target_table_name_original.lower(),
                    "target_column_name_original": fk_target_column_name_original.lower(),
                }
            )
        db_schemas[db["db_id"]]["fk"] = foreign_keys

        db_schemas[db["db_id"]]["schema_items"] = []
        for idx, table_name_original in enumerate(table_names_original):
            column_names_original_list = []
            column_names_list = []
            column_types_list = []
            for column_idx, (table_idx, column_name_original) in enumerate(column_names_original):
                if idx == table_idx:
                    column_names_original_list.append(
                        column_name_original.lower())
                    column_names_list.append(
                        column_names[column_idx][1].lower())
                    column_types_list.append(column_types[column_idx])

            db_schemas[db["db_id"]]["schema_items"].append({
                "table_name_original": table_name_original.lower(),
                "table_name": table_names[idx].lower(),
                "column_names": column_names_list,
                "column_names_original": column_names_original_list,
                "column_types": column_types_list
            })

    # return db_schemas
    db_schema = db_schemas[db_name]
    db_schema_str = ""
    # 列信息
    for i in db_schema['schema_items']:
        db_schema_str += i['table_name']+'('
        for j in i['column_names']:
            db_schema_str += j + ','
        db_schema_str = db_schema_str[:-1]+")\n"
    # 主键信息
    db_schema_str += "primary_keys("
    for i in db_schema['pk']:
        db_schema_str += i['table_name_original'] + \
            '.'+i['column_name_original'] + ','
    db_schema_str = db_schema_str[:-1]+')\n'
    # 外键信息
    db_schema_str += "foreign_keys("
    for i in db_schema['fk']:
        db_schema_str += i['source_table_name_original']+'.'+i['source_column_name_original'] + \
            '='+i['target_table_name_original']+'.' + \
            i['target_column_name_original'] + ','
    db_schema_str = db_schema_str[:-1]+')\n'

    # db_schema_str += '\n'
    return db_schema_str


table_path = "/Users/xianyiran/Desktop/实验室/text2sql/spider/tables.json"
all_db_infos = json.load(open(table_path))
print(get_db_schemas(all_db_infos, "dog_kennels"))
