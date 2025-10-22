from verification_03_30_util import call_gpt
import copy

# def revision_logic_error(question, hint, candidate_sql, multiple_sqls, feedback, schemas):
def revision_logic_error(question, hint, candidate_sql, feedback, schemas, model_name='deepseek-chat'):
    PROMPT_REVISION_LOGIC = """
## Task
# If the SQL query deviates from the intended logic, revise it to reflect the correct semantic meaning.

## Problem Type
# Overall Semantic Logic Deviation

## Question
{}
## Hint: 
{} 

## SQL Query: 
{}

## Possible Error Causes and Suggestions:
{}

## Schema Information:
{}

There are some tips:
1. Double negatives imply a positive. For example, available outside of the United States refers to isForeignOnly = 1, so 'not available outside of the United States' means isForeignOnly = 0.
2. You must determine Whether the columns returned by the SQL Query fully match those in the NL Question. If the SQL query consists of multiple sub-queries formed with "WITH", focus on whether the content returned by the final sub-query matches the NL Question requirements.
The order of returned columns in the SQL can differ from the order in the question.

## Please revise the SQL according to the Possible Error Causes and Suggestions, and the Schema Information. If you think there are no errors, output the original SQL query unchanged.

Please directly output the revised SQL without any explanation.
Revision:
"""
    # PROMPT_REVISION_LOGIC = PROMPT_REVISION_LOGIC.format(question, hint, candidate_sql, multiple_sqls, feedback, schemas)
    PROMPT_REVISION_LOGIC = PROMPT_REVISION_LOGIC.format(question, hint, candidate_sql, feedback, schemas)
    prompt_revision_logic_error, revised_sql, input_cost, output_cost = call_gpt(PROMPT_REVISION_LOGIC, model_name=model_name)
    print(f"prompt of revision_logic_error:\n{prompt_revision_logic_error}")
    print(f"revised_sql:\n{revised_sql}")
    return revised_sql, input_cost, output_cost


# def revision_percentage_error(question, hint, candidate_sql, multiple_sqls, feedback, schemas):
def revision_percentage_error(question, hint, candidate_sql, feedback, schemas, model_name='deepseek-chat'):
    PROMPT_PERCENTAGE_ERROR = """
## Task
# Review the SQL query for percentage-related errors, focusing on questions about percentage calculations. 
# If errors are found, output a corrected SQL query based on the Question, Possible Error Causes and Suggestions, and Schema.
# If no errors are found, return the original SQL query unchanged.

## Question: 
{}
## Hint: 
{} 

## SQL Query: 
{}

## Possible Error Causes and Suggestions:
{}

## Schema Information:
{}

There are some tips:
1. Double negatives imply a positive. For example, available outside of the United States refers to isForeignOnly = 1, so 'not available outside of the United States' means isForeignOnly = 0.
2. You must determine Whether the columns returned by the SQL Query fully match those in the NL Question. If the SQL query consists of multiple sub-queries formed with "WITH", focus on whether the content returned by the final sub-query matches the NL Question requirements.
The order of returned columns in the SQL can differ from the order in the question.

## Please revise the SQL according to the Possible Error Causes and Suggestions, and the Schema Information. If you think there are no errors, output the original SQL query unchanged.

Please directly output the revised SQL without any explanation.
Revision:
"""
    # PROMPT_PERCENTAGE_ERROR = PROMPT_PERCENTAGE_ERROR.format(question, hint, candidate_sql, multiple_sqls, feedback, schemas)
    PROMPT_PERCENTAGE_ERROR = PROMPT_PERCENTAGE_ERROR.format(question, hint, candidate_sql, feedback, schemas)
    prompt_percentage_error, revised_sql, input_cost, output_cost = call_gpt(PROMPT_PERCENTAGE_ERROR, model_name=model_name)
    print(f"prompt of percentage_error:\n{prompt_percentage_error}")
    print(f"revised_sql:\n{revised_sql}")
    return revised_sql, input_cost, output_cost

# def revision_schema_error(question, hint, candidate_sql, multiple_sqls, feedback, schemas, filtered_tables):
def revision_schema_error(question, hint, candidate_sql, feedback, schemas, filtered_tables, model_name='deepseek-chat'):
    PROMPT_REVISION_SCHEMA = """
## Task
# Review the SQL query for schema-related errors, focusing on table joins (or the format of 'tab1.col1 IN (SELECT col1 FROM tab2)', i.e., using the 'IN' operator) and column usage. the join columns (or 'IN' operator) must match the primary-foreign key relationships provided in "Schema Information".
# If errors are found, output a corrected SQL query that uses the correct tables, joins, and columns based on the Possible Error Causes and Suggestions, and Schema.
# If no errors are found, return the original SQL query unchanged.

## Problem Type
# Schema-Related Errors (Table Joins and Column Use)

## SQL Query: 
{}

## Possible Error Causes and Suggestions:
{}

## Schema Information:
{}

## Tables must in SQL:
{}

Tips: You must only correct errors related to the database schema. Apart from this, you cannot modify anything else, such as predicate values or returned columns.

## Please revise the SQL according to the Possible Error Causes and Suggestions, Schema Information and the Tables must in SQL. If you think there are no errors, output the original SQL query unchanged.
Please directly output the revised SQL without any explanation.

Revision:
"""
    # PROMPT_REVISION_SCHEMA = PROMPT_REVISION_SCHEMA.format(question, hint, candidate_sql, feedback, schemas, filtered_tables)
    PROMPT_REVISION_SCHEMA = PROMPT_REVISION_SCHEMA.format(candidate_sql, feedback, schemas, filtered_tables)
    prompt_revision_schema_error, revised_sql, input_cost, output_cost = call_gpt(PROMPT_REVISION_SCHEMA, model_name=model_name)
    print(f"prompt of revision_schema_error:\n{prompt_revision_schema_error}")
    print(f"revised_sql:\n{revised_sql}")
    return revised_sql, input_cost, output_cost



# def revision_db_value_error(question, hint, candidate_sql, multiple_sqls, samples_data, similar_values):
def revision_db_value_error(question, hint, candidate_sql, samples_data, similar_values, model_name='deepseek-chat'):
    PROMPT_REVISION_VALUE = """
## Task  
# Review the SQL query for issues related to predicate values, specifically focusing on database value usage error including incorrect formats and mismatched content.
# If errors are found, output a corrected SQL query that uses the correct database value.
# If no issues are found, return the original SQL query unchanged.  

## Problem Type  
# Predicate Value Errors  

There are some tips:
1. The columns following GROUP BY and ORDER BY must be the same or have the same conditions. If a column used in ORDER BY is of type TEXT but contains numeric values, it is necessary to use CAST(col AS REAL) to convert the data type.
2. The columns returned for the question about 'top three elements' must include DISTINCT.


## SQL Query: 
{}

## Database Sampling Information: 
{}  

{}

## Please revise the SQL according to the Database Sampling Information and the relevant similar values. If you think there are no errors, output the original SQL query unchanged.
Please directly output the revised SQL without any explanation.

Revision: 
"""
    # if len(similar_values) == 0:
    #     PROMPT_REVISION_VALUE = PROMPT_REVISION_VALUE.format(candidate_sql, multiple_sqls, samples_data, "")
    # else:
    #     PROMPT_REVISION_VALUE = PROMPT_REVISION_VALUE.format(candidate_sql, multiple_sqls, samples_data, f"## Similar Values:\n{similar_values}")
    if len(similar_values) == 0:
        PROMPT_REVISION_VALUE = PROMPT_REVISION_VALUE.format(candidate_sql, samples_data, "")
    else:
        PROMPT_REVISION_VALUE = PROMPT_REVISION_VALUE.format(candidate_sql, samples_data, f"## Similar Values:\n{similar_values}")
    prompt_revision_db_value_error, revised_sql, input_cost, output_cost = call_gpt(PROMPT_REVISION_VALUE, model_name=model_name)
    print(f"prompt of revision_db_value_error:\n{prompt_revision_db_value_error}")
    print(f"revised_sql:\n{revised_sql}")
    return revised_sql, input_cost, output_cost



def revision_execution_error(question, hint, candidate_sql, feedback, schemas, model_name='deepseek-chat'):
    PROMPT_REVISION_EXEC_ERROR = """
## Task  
# Revised the SQL query based on the error information.

## Problem Type  
# Predicate Value Errors  

## Question: 
{}  
## Hint: 
{}  

## SQL Query: 
{}  

## Error Information: 
{}  

## Schema Information:
{}

There are some tips: 
1. If an execution error occurs due to a missing column, refer to the evidence about whether a related column name exists. If the column name contains spaces or hyphens (-), enclose it in backquote, e.g., `Examination Date`, `T-BIL`, as this is the correct SQL syntax.
2. If a column in the SQL does not exist in the provided schema, but there are clear hints in the question and hint, do not remove it.

## Please output the revised SQL without any explanation
# Revised SQL:  
"""
    PROMPT_REVISION_EXEC_ERROR = PROMPT_REVISION_EXEC_ERROR.format(question, hint, candidate_sql, feedback, schemas)
    prompt_revision_execution_error, revised_sql, input_cost, output_cost = call_gpt(PROMPT_REVISION_EXEC_ERROR, model_name=model_name)
    print(f"prompt of revision_execution_error:\n{prompt_revision_execution_error}")
    print(f"revised_sql:\n{revised_sql}")
    return revised_sql, input_cost, output_cost



def postprocess(one_sql):
    tmp_comb_sql = copy.deepcopy(one_sql)
    if tmp_comb_sql is None:
        return tmp_comb_sql
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
    return tmp_comb_sql