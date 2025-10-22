import json
import openai
import time


BASE_URL_OPENAI = 'https://api.openai.com/v1'
OPENAIKEY=''

DSKEY=''
BASE_URL_DS='https://api.deepseek.com'



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


## 5 个例子
def select_decomposition_template(question, evidence, schemas, model='deepseek-reasoner'):
    select_prompt = '''
## Given a question with corresponding hint, you need to decompose it into sub questions, and output the decomposed sub questions.

Here are some examples:

Question: Calculate the percentage of movie titles with a screen length of more than 120 minutes that have a category of horror movies. 
Hint: screen length of more than 120 minutes refers to length > 120; category of horror refers to category.name = 'Horror'; percentage = divide(count(title where length > 120 and category.name = 'Horror'), count(title)) * 100%
Sub-Question 1: Calculate the count of movie titles with a screen length of more than 120 minutes that have a category of horror movies. -> 'ans1'
Sub-Question 2: Calculate the count of movies. -> 'ans2'
Sub-Question 3: Return 'ans1' * 100 / 'ans2'. -> (divide(count(title where length > 120 and category.name = 'Horror'), count(title)) * 100%)

Question: What is the ratio between male and female cast members of the movie 'Iron Man?' Count how many have unspecified genders.
Hint: male refers to gender = 'Male'; female refers to gender = 'Female'; movie 'Iron Man' refers to title = 'Iron Man'; ratio = divide(sum(gender = 'Male'), sum(gender = 'Female'))
Sub-Question 1:  Calculate the count of male cast members of the movie 'Iron Man?', and the number of unspecified genders. -> 'ans1'
Sub-Question 2:  Calculate the count of female cast members of the movie 'Iron Man?', and the number of unspecified genders. -> 'ans2'
Sub-Question 3: Return 'ans1' / 'ans2', and the number of unspecified genders. -> (divide(sum(gender = 'Male'), sum(gender = 'Female')))

Question: For the quantities, what percent more did the store in Fremont sell than the store in Portland in 1993? 
Hint: qty is abbreviation for quantity; Fremont and Portland are name of city; sell in 1993 refers to YEAR(ord_date) = 1993; percentage = DIVIDE(\nSUBTRACT(SUM(qty where city =  2018Fremont 2019 and year(ord_date = 1993)), \nSUM(qty where city =  2018Portland 2019 and year(ord_date = 1993))), SUM(qty where city =  2018Fremont 2019 and year(ord_date = 1993)) *100
Sub-Question 1: For the quantities, calculate the count of store in Fremont in 1993. -> 'ans1'
Sub-Question 2: For the quantities, calculate the count of store in Portland in 1993. -> 'asn2'
Sub-Question 3: Return ('ans1' - 'ans2') * 100 / 'ans1' -> (DIVIDE(SUBTRACT(SUM(qty where city =  2018Fremont 2019 and year(ord_date = 1993)), SUM(qty where city =  2018Portland 2019 and year(ord_date = 1993))), SUM(qty where city =  2018Fremont 2019 and year(ord_date = 1993)) *100%)
Chain-of-thought: 'For the quantities' is the constraint condition, you should maintain it in 'Sub-Question 1' and 'Sub-Quesiton 2', which cannot be discarded. 

Question: Among the students enrolled in UCLA, what is the percentage of male students in the air force department? 
Hint:  UCLA refers to school = 'ucla'; percentage = MULTIPLY(DIVIDE(COUNT(male.name), COUNT(person.name)), 100); male students are mentioned in male.name; department refers to organ; organ = 'air_force';
Sub-Question 1: Among the students enrolled in UCLA, Calculate the count of male students in the air force department. -> 'ans1'
Sub-Question 2: Among the students enrolled in UCLA, Calculate the count of all students in the air force department. -> 'ans2'
Sub-Question 3: Return 'ans1' * 100 / 'ans2'. -> DIVIDE(COUNT(male.name), COUNT(person.name)) * 100%
Chain-of-thought: 'Among the students enrolled in UCLA' is the constraint condition, you should maintain it in 'Sub-Question 1' and 'Sub-Quesiton 2', which cannot be discarded. 

Question: What is the percentage of rated movies were not released in year 2021? 
Hint: percentage = DIVIDE(SUM(movie_release_year != 2021), COUNT(rating_id)) as percent; movies released in year 2021 refers to movie_release_year = 2021;
Sub-Question 1: What is number/count of rated movies were not released in year 2021. -> 'ans1'
Sub-Question 2: What is number/count of rated movies. -> 'ans2'
Sub-Question 3: Return ('ans1' / 'ans2') -> DIVIDE(SUM(movie_release_year != 2021), COUNT(rating_id))
Chain-of-thought: You cannot decompose the first sub-question as `the number of rated movies were released in 2021`, which differs from the original meaning. 

For the following question and hint, you need to decompose the question into two sub-questions.
You need to notice that:
1. Generally, multiply the numerator by 100, i.e., 100 * ans1 / ans2 .And determine whether to multiply by 100 based on the hint.
2. If you think the question cannot be decomposed into sub-questions, and the hint does not provide an explicit formula guiding the decomposition, you just output 'No decomposition'.
3. You must ensure that the sub-questions cannot ignore any information from the original question. For the clause that indicates the constraint condition, you should maintain it in the sub-questions.

Question:{}
Hint: {}
'''
    select_prompt = select_prompt.format(question, evidence)
    print(f"select_prompt:{select_prompt}")
    max_cnt = 15
    cur_cnt = 0
    while cur_cnt < max_cnt:
        try:
            if 'deepseek' in model:
                res, input_cost, output_cost = ask_llm(model, select_prompt, BASE_URL_DS, DSKEY)
            else:
                res, input_cost, output_cost = ask_llm(model, select_prompt, BASE_URL_OPENAI, OPENAIKEY)
            return res, input_cost, output_cost
        except Exception as e:
            cur_cnt+=1
            print(e)
            continue
    exit(-1)


##
def indep_decomposition_template(question, evidence, schemas, model='deepseek-chat'):
    indep_prompt = '''
## Given you a question , you need to decompose the original complex question into three sub questions, and output the decomposed sub questions.
The original question may have redundant clauses. You need to refine the sub-qeustions.

Here are some examples:

Question: Which two products has the highest and lowest expected profits? Determine the total price for each product in terms of the largest quantity that was ordered. 
Hint: expected profits = SUBTRACT(msrp, buyPrice); total price = MULTIPLY(quantityOrdered, priceEach).
Sub-Question 1: Return the products with highest expected profits, and the corresponding total price for each product in terms of the largest quantity that was ordered. -> 'ans1'
Sub-Question 2: Return the products with lowest expected profits, and the corresponding total price for each product in terms of the largest quantity that was ordered. -> 'ans2'
Sub-Question 3: Return 'ans1' and (union) 'ans2'.


For the following question and hint, you need to decompose the question into two sub-questions.
You need to notice that:
1. The 'Sub-Question 1' and 'Sub-Question 2' are independent each other, and 'Sub-Question 3' combines the answer of 'Sub-Question 1' and 'Sub-Question 2'. --> combination type can be 'UNION', 'INTERSECTION', 'EXCEPT'
2. You must ensure that the sub-questions cannot ignore any information from the original question.
3. If you think the question cannot be decomposed into sub-questions, you just output 'No decomposition'.

Question:{}
Hint: {}

'''
    indep_prompt = indep_prompt.format(question, evidence)
    print(f"indep_prompt:{indep_prompt}")
    max_cnt = 15
    cur_cnt = 0
    while cur_cnt < max_cnt:
        try:
            if 'deepseek' in model:
                res, input_cost, output_cost = ask_llm(model, indep_prompt, BASE_URL_DS, DSKEY)
            else:
                res, input_cost, output_cost = ask_llm(model, indep_prompt, BASE_URL_OPENAI, OPENAIKEY)
            return res, input_cost, output_cost
        except Exception as e:
            cur_cnt+=1
            print(e)
            continue
    exit(-1)




# 找5个例子
def from_decomposition_template(question, evidence, schemas, model='deepseek-chat'):
    from_prompt = '''
## Given you a question , you need to decompose the original complex question into two sub questions, and output the decomposed sub questions.
The original question may have redundant clauses. You need to refine the sub-qeustions.


Here are some examples:

Question: Give the number of stores which opened on the weather station that recorded the fastest average wind speed.
Hint:  fastest average wind speed refers to Max(avgspeed); number of store refers to count(store_nbr)
Sub-Question 1: Return the weather station that recorded the fastest average wind speed. -> 'ans1'
Sub-Question 2: Give the number stores which opened on the weather station in 'ans1'.


Question: What is the max time in seconds of champion for each year? 
Hint: only champion's finished time is represented by 'HH:MM:SS.mmm'; finished the game refers to time is not null.
Sub-Question 1: List the time in seconds for all champions. -> 'ans1'
Sub-Question 2: What is the max time in 'ans1' group by year.
Chain-of-Thought: We can first calculate the time in seconds for each champion of all data. Then, for all champions, we group them by year, and return the max time for each year.


Question: What is the name of the customer who purchased the product with the highest net profiit?
Hint: highest net profit = Max(Subtract (Unit Price, Unit Cost)); name of customer refers to Customer Names
Sub-Question 1: What is the name of the customer and their net profit of the purchased product. -> 'ans1'
Sub-Question 2: What is the name of the customer who purchased the product with the highest net profiit in 'ans1'


Question: Identify the name and product category for the most expensive and the least expensive products.
Hint: name of product refers to ProductName; category of product refers to CategoryName; the most expensive products refers to MAX(UnitPrice); the least expensive products refers to MIN(UnitPrice);
Sub-Question 1: What is price of the most expensive and the least expensive products. -> 'ans1'
Sub-Question 2: Identify the name and product category with the price in 'ans1'


Question: What is the name of the category which most users belong to?
Hint: most users belong to refers to MAX(COUNT(app_id)); name of category refers to category;
Sub-Question 1: What is the name of the category and their number of users belong to. -> 'ans1'
Sub-Question 2: What is the name of the category which most users belong to in 'ans1'?




For the following question and hint, you need to decompose the question into two sub-questions.
You need to notice that:
1. Each sub-question depends on its previous sub-question, i.e., the result of 'Sub-Question 1' is the input of 'Sub-Question 2'. Thus, the final sub-question represents the original question. 
'Sub-Question 1' generally corresponds to the part after `FROM` in SQL, and its result usually corresponds to a sub-table.
2. You must ensure that the sub-questions cannot ignore any information from the original question.
3. If you think the question cannot be decomposed into sub-questions, you just output 'No decomposition'.


Question:{}
Hint: {}
'''
    from_prompt = from_prompt.format(question, evidence)
    print(f"from_prompt:{from_prompt}")
    max_cnt = 15
    cur_cnt = 0
    while cur_cnt < max_cnt:
        try:
            if 'deepseek' in model:
                res, input_cost, output_cost = ask_llm(model, from_prompt, BASE_URL_DS, DSKEY)
            else:
                res, input_cost, output_cost = ask_llm(model, from_prompt, BASE_URL_OPENAI, OPENAIKEY)
            return res, input_cost, output_cost
        except Exception as e:
            cur_cnt+=1
            print(e)
            continue
    exit(-1)


#找5个例子
def where_decomposition_template(question, evidence, schemas, model='deepseek-chat'):
    where_prompt = '''
## Given you a question , you need to decompose the original complex question into two sub questions, and output the decomposed sub questions.
The original question may have redundant clauses. You need to refine the sub-qeustions.


Here are some examples:
    
Question: Among games sold in Europe, list the platform ID of games with sales lesser than 30% of the average number of sales. 
Hint: Europe refers to region_name = 'Europe'; sales lesser than 30% of the average number of sales refers to SUM(num_sales) < MULTIPLY(AVG(num_sales), 0.3);
Sub-Question 1: Among games sold in Europe, Calculate 30% of the average number of sales. -> 'ans1'
Sub-Question 2: Among games sold in Europe, list the platform ID of games with sales lesser than 'ans1'.
Chain-of-Thought: Note that the condition 'Among games sold in Europe' in both 'Sub-Question 1' and 'Sub-Question 2', as this condition restrict the type of games.


Question: Among the males, list the region name of people with height greater than 87% of the average height of all people listed.
Hint: males refer to gender = 'M'; height greater than 87% of the average height refers to height > MULTIPLY(AVG(height), 0.87);
Sub-Question 1: Calculate 87% of the average height of all people listed. -> 'ans1'
Sub-Question 2: Among the males, list the region name of people with height greater than 'ans1'.


Question: What is the name of the CBSA of the city with the highest average house value?
Hint: the highest average house value refers to avg_house_value;
Sub-Question 1: What is the highest average house value. -> 'ans1'
Sub-Question 2: What is the name of the CBSA of the city with the average house value in 'ans1'.


Question: List the driver's name of the shipment with a weight greater than 95% of the average weight of all shipments.
Hint: weight greater than 95% of average weight refers to weight > Multiply (AVG(weight), 0.95); driver name refers to first_name, last_name;
Sub-Question 1: Calculate the 95% of the average weight of all shipments. -> 'ans1'
Sub-Question 2: List the driver's name of the shipment with a weight greater than 'ans1'.


Question: What is the name of the CBSA of the city with the highest average house value?
Hint: the highest average house value refers to avg_house_value;
Sub-Question 1: What is the highest average house value. -> 'ans1'
Sub-Question 2: What is the name of the CBSA of the city with the average house value in 'ans1'.




For the following question and hint, you need to decompose the question into two sub-questions.
You need to notice that:
1. Each sub-question depends on its previous sub-question, i.e., the result of 'Sub-Question 1' is the input of 'Sub-Question 2'. Thus, the final sub-question represents the original question. 
'Sub-Question 1' generally corresponds to the part after `WHERE` in SQL, and its result usually corresponds to a value or a sub-table.
2. You must ensure that the sub-questions cannot ignore any information from the original question.
3. If you think the question cannot be decomposed into sub-questions, you just output 'No decomposition'.


Question:{}
Hint: {}
    
    '''
    
    where_prompt = where_prompt.format(question, evidence)
    print(f"where_prompt:{where_prompt}")
    max_cnt = 15
    cur_cnt = 0
    while cur_cnt < max_cnt:
        try:
            if 'deepseek' in model:
                res, input_cost, output_cost = ask_llm(model, where_prompt, BASE_URL_DS, DSKEY)
            else:
                res, input_cost, output_cost = ask_llm(model, where_prompt, BASE_URL_OPENAI, OPENAIKEY)
            return res, input_cost, output_cost
        except Exception as e:
            cur_cnt+=1
            print(e)
            continue
    exit(-1)


def decomposition_prompt(question,evidence, schemas, model='deepseek-chat'):
    indep_ans = "No decomposition"
    select_ans = "No decomposition"
    from_ans = "No decomposition"
    where_ans = "No decomposition"
    input_cost = 0
    output_cost = 0
    indep_ans, tmp_input_cost, tmp_output_cost = indep_decomposition_template(question, evidence, schemas, model=model)
    input_cost += tmp_input_cost
    output_cost += tmp_output_cost

    select_ans, tmp_input_cost, tmp_output_cost = select_decomposition_template(question, evidence, schemas, model=model)
    input_cost += tmp_input_cost
    output_cost += tmp_output_cost

    from_ans, tmp_input_cost, tmp_output_cost = from_decomposition_template(question, evidence, schemas, model=model)
    input_cost += tmp_input_cost
    output_cost += tmp_output_cost
    
    where_ans, tmp_input_cost, tmp_output_cost = where_decomposition_template(question, evidence, schemas, model=model)
    input_cost += tmp_input_cost
    output_cost += tmp_output_cost
    

    return {
        'indep_ans':indep_ans,
        'select_ans':select_ans,
        'from_ans':from_ans,
        'where_ans': where_ans,
        'decompose_input_cost': input_cost,
        'decompose_output_cost': output_cost
    }
