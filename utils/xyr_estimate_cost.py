from transformers import GPT2Tokenizer
import json

input_path = "/Users/xianyiran/Desktop/实验室/text2sql/decomposition_and_DAIL/extra_dev.json"
tokens_len = 0
tokenizer = GPT2Tokenizer.from_pretrained("gpt2")

with open(input_path, 'r') as f:
    data = json.load(f)

for i in data:
    tokens_len = 0
    for j in range(4):
        len(tokenizer.encode(prompt))

prompt = "###I need you to decompose the original complex question into two sub questions, and output the decomposed sub questions.\n\n"
prompt = prompt + "Question: Show the status of the city that has hosted the greatest number of competitions.\n \
        Sub-Question1: Show the status of the city and the number of competitions they host.\n \
        Sub-Question2: Show the status of the city that has hosted the greatest number of competitions in 'tb1'. \n \
                    \
        Question: which course has most number of registered students?\n \
        Sub-Question1: show courses and the number of registered students of each course.\n \
        Sub-Question2: which course has most number of rigistered students in 'tb1'?\n\
                    \
        Question: List the state in the US with the most invoices.\n\
        Sub-Question1: List the states and number of invoices in the US.\n\
        Sub-Question2: List the state in the US with the most invoices in 'tb1'.\n\
                    \
        Question: Return the structure description of the document that has been accessed the fewest number of times.\n\
        Sub-Question1: Return the structure description of the document and the number of times it was acessed .\n\
        Sub-Question2: Return the structure description of the document that has been accessed the fewest number of times in 'tb1'.\n\
                    \
        Question: Find the claim that has the largest total settlement amount. Return the effective date of the claim.\n\
        Sub-Question1: Find the effective date of the claim and the total settlement amount of each claim?\n\
        Sub-Question2: Return the effective date of the claim that has the largest total settlement amount in 'tb1'.\n\
                    \
        Question: What is the id and name of the enzyme with most number of medicines that can interact as 'activator'?\n\
        Sub-Question1: What is the id and name and the number of medicines of the enzyme that can interact as 'activator'?\n\
        Sub-Question2: What is the id and name of the enzyme with most number of medicines in 'tb1'?\n\
                    \
        Question: List the name of the phone model launched in year 2002 and with the highest RAM size.\n\
        Sub-Question1: List the name of the phone model and their RAM size launched in year 2002.\n\
        Sub-Question2: List the name of the phone model with the highest RAM size in 'tb1'.\n\
                    \
        Question: Show the most common apartment type code among apartments with more than 1 bathroom.\n\
        Sub-Question1: Lists the apartment type codes in apartments with more than 1 bathroom and the number of apartments corresponding to each type.\n\
        Sub-Question2: Show the most common apartment type code in 'tb1'.\n\
                    \
        Question: Find the name and id of the team that won the most times in 2008 postseason.\n\
        Sub-Question1: Find the name, id, and number of wins for the 2008 postseason.\n\
        Sub-Question2: Find the name and id of the team that won the most times in 'tb1'.\n\
                    \
        Question: Show the name of track with most number of races.\n\
        Sub-Question1: Show the name of tracks and their number of races.\n\
        Sub-Question2: Show the name of track with most number of races in 'tb1'.\n\n"  # bridge type :sub question 在from clause
prompt = "###I need you to decompose the original complex question into two sub questions, and output the decomposed sub questions.\n\n "
prompt = prompt + "Question: What is the average bike availability in stations that are not located in Palo Alto?\n\
        Sub-Question1: Which stations located in Palo Alto?\n\
        Sub-Question2: What is the average bike availabiliy in stations which not in 'tb1'?\n\
                    \
        Question: How many customers do not have an account?\n\
        Sub-Question1: What are the ids of customers who has an account?\n\
        Sub-Question2: How many customers that not in 'tb1'?\n\
                    \
        Question: List the first and last name of the students who do not have any food type allergy.\n\
        Sub-Question1: List the students who have food type allergy.\n\
        Sub-Question2: List the first and last name of the students who is not in 'tb1'.\n\
                    \
        Question: Find the average rating star for each movie that are not reviewed by Brittany Harris.\n\
        Sub-Question1: Find the movie that are reviewed by Brittany Harris.\n\
        Sub-Question2: Find the average rating star for each movie that not in 'tb1'.\n\
                    \
        Question: How many enzymes do not have any interactions?\n\
        Sub-Question1: Find the enzymes that have interactions.\n\
        Sub-Question2: How many enzymes that not in 'tb1'.\n\
                    \
        Question: How many schools do not participate in the basketball match?\n\
        Sub-Question1: List the schools that participate in the basketball match.\n\
        Sub-Question2: How many schools that not in 'tb1'?\n\
                    \
        Question: Find the average ram mib size of the chip models that are never used by any phone.\n\
        Sub-Question1: Find the ram mib size of the chip models that are used by any phone.\n\
        Sub-Question2: Find the average ram mib size of the chip models not in 'tb1'.\n\n"  # bridge type not
prompt = "###I need you to decompose the original complex question into two sub questions, and output the decomposed sub questions.\n\n "
prompt = prompt + "Question: On which day and in which zip code was the min dew point lower than any day in zip code 94107?\n\
        Sub-Question1: Find the min dew point in zip code 94107.\n\
        Sub-Question2: On which day and in which zip code the min dew point was less than 'value1'?\n\
                    \
        Question: Return ids of all the products that are supplied by supplier id 2 and are more expensive than the average price of all products.\n\
        Sub-Question1: Return the average price of all products.\n\
        Sub-Question2: Return ids of all the products that are supplied by supplier id 2 and are more expensive than 'value1'.\n\
                    \
        Question: Find the team names of the universities whose enrollments are smaller than the average enrollment size.\n\
        Sub-Question1: Find the average enrollment size.\n\
        Sub-Question2 : Find the team names of the universities whose enrollments are smaller than 'value1'.\n\
                    \
        Question: Show the name, location, open year for all tracks with a seating higher than the average.\n\
        Sub-Question1: Show the average seating of all tracks.\n\
        Sub-Question2: Show the name, location, open year for all tracks with a seating higher than 'value1'.\n\n"  # bridge type where
prompt = "###I need you to decompose the original complex question into three sub questions, and output the decomposed sub questions.\n\n "
prompt = prompt + "Question: List the states where both the secretary of 'Treasury' department and the secretary of 'Homeland Security' were born.\n\
        Sub-Question1: List the states where the secretary of 'Treasury' department was born.\n\
        Sub-Question2: List the states where the secretary of 'Homeland Security' was born.\n\
        Sub-Question3: List the states both in 'tb1' and 'tb2'.\n\
                \
        Question: What are the names and ids of stations that had more than 14 bikes available on average or were installed in December?\n\
        Sub-Question1: What are the names and ids of stations that had more than 14 bikes available on average?\n\
        Sub-Question2: What are the names and ids of stations that were installed in December?\n\
        Sub-Question3: What are the names and ids that both in 'tb1' and 'tb2'?\n\
                \
        Question: What are the names of all stations that have more than 10 bikes available and are not located in San Jose?\n\
        Sub-Question1: What are the names of all stations that have more than 10 bikes available?\n\
        Sub-Question2: What are the names of all stations that are located in San Jose?\n\
        Sub-Question3: What are the names of all stations that in 'tb1' but not in 'tb2'?\n\
                \
        Question: Find the name of tracks which are in Movies playlist but not in music playlist.\n\
        Sub-Question1: Find the name of tracks which are in Movies playlist.\n\
        Sub-Question2: Find the name of tracks which are in music playlist.\n\
        Sub-Question3: Find the name of tracks which are in 'tb1' but not in 'tb2'.\n\
                \
        Question: Show the shipping charge and customer id for customer orders with order status Cancelled or Paid.\n\
        Sub-Question1: Show the shipping charge and customer id for customer orders with order status Cancelled.\n\
        Sub-Question2: Show the shipping charge and customer id for customer orders with order status Paid.\n\
        Sub-Question3: Show the shipping charge and customer id who is in 'tb1' or 'tb2'.\n\
                \
        Question: Find the name and location of the stadiums which some concerts happened in the years of both 2014 and 2015.\n\
        Sub-Question1: Find the name and location of the stadiums which some concerts happened in the years of 2014.\n\
        Sub-Question2: Find the name and location of the stadiums which some concerts happened in the years of 2015.\n\
        Sub-Question3: Find the name and location of the stadiums who is both in 'tb1' and 'tb2'.\n\
                \
        Question: Find the names of customers who have used both the service \"Close a policy\" and the service \"New policy application\".\n\
        Sub-Question1: Find the names of customers who have the service \"Close a policy\".\n\
        Sub-Question2: Find the names of customers who have the service \"Close a policy\".\n\
        Sub-Question3: Find the names of customers who is in both 'tb1' and 'tb2'.\n\
                    \
        Question: List the medicine name and trade name which can both interact as 'inhibitor' and 'activitor' with enzymes.\n\
        Sub-Question1: List the medicine name and trade name which can interact as 'inhibitor' with enzymes.\n\
        Sub-Question2: List the medicine name and trade name which can interact as 'activitor' with enzymes.\n\
        Sub-Question3: List the medicine name and trade that both in 'tb1' and 'tb2'.\n\
                    \
        Question: Find the pixels of the screen modes that are used by both phones with full accreditation types and phones with Provisional accreditation types.\n\
        Sub-Question1: Find the pixels of the screen modes that are used by phones with full accreditation types.\n\
        Sub-Question2: Find the pixels of the screen modes that are used by phones with Provisional accreditation types.\n\
        Sub-Question3: Find the pixels of the screen modes that both in 'tb1' and 'tb2'.\n\n"
total_len = len(tokenizer.encode(prompt))
print(total_len)
