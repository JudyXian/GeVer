import sqlite3

db_path = '/Users/xianyiran/Desktop/实验室/text2sql/spider/database/'

db_name = "world_1"
path = db_path+f"{db_name}/{db_name}.sqlite"
print(path)
conn = sqlite3.connect(path)
cs = conn.cursor()

cs.execute('select t1.name from country as t1 join countrylanguage as t2 on t1.code  =  t2.countrycode where t2.language  =  \"english\" and isofficial  =  \"t\" union select t1.name from country as t1 join countrylanguage as t2 on t1.code  =  t2.countrycode where t2.language  =  \"dutch\" and isofficial  =  \"t\"')
result = cs.fetchall()
print(result)
