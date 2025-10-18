import sqlite3

db_path = '/home3/xianyiran/text2sql/spider/database/'

db_name = "bike_1"
path = db_path+f"{db_name}/{db_name}.sqlite"
print(path)
conn = sqlite3.connect(path)
cs = conn.cursor()

# db_name = "student_assessment"
# cs.execute('SELECT T1.course_name FROM courses AS T1 JOIN student_course_registrations AS T2 ON T1.course_id = T2.course_Id GROUP BY T1.course_id ORDER BY count(*) DESC LIMIT 1')
# revised  可以改成nested query 从而进一步decompose
# cs.execute('SELECT T1.course_name FROM courses AS T1 WHERE T1.course_id IN (SELECT T2.course_Id FROM student_course_registrations AS T2) GROUP BY T1.course_id ORDER BY count(*) DESC LIMIT 1')

# db_name = "bike_1"
# cs.execute('SELECT avg(bikes_available) FROM status WHERE station_id NOT IN (SELECT id FROM station WHERE city  =  \"Palo Alto\")')
# revised 改成IN就不对了， 因为status和 station两个表中的id不完全一样
# cs.execute('SELECT avg(bikes_available) FROM status WHERE station_id IN (SELECT id FROM station WHERE city  !=  \"Palo Alto\")')


# cs.execute('SELECT T1.product_id FROM product_suppliers AS T1 JOIN products AS T2 ON T1.product_id  =  T2.product_id WHERE T1.supplier_id  =  2 AND T2.product_price  >  (SELECT avg(product_price) FROM products)')
# revised 
cs.execute('SELECT T1.product_id FROM product_suppliers AS T1 WHERE T1.supplier_id  =  2 AND T1.product_id IN (SELECT T2.product_id FROM products AS T2 WHERE T2.product_price > (SELECT avg(product_price) FROM products))')

# cs.execute('SELECT T1.product_id FROM product_suppliers AS T1 WHERE T1.supplier_id  =  2')
# cs.execute('SELECT T1.product_id FROM product_suppliers AS T1 WHERE T1.product_id IN (SELECT T2.product_id FROM products AS T2 WHERE T2.product_price > (SELECT avg(product_price) FROM products))')
# revised more 可以进一步decompose
# cs.execute('SELECT T1.product_id FROM product_suppliers AS T1 WHERE T1.supplier_id  =  2 INTERSECT SELECT T1.product_id FROM product_suppliers AS T1 WHERE T1.product_id IN (SELECT T2.product_id FROM products AS T2 WHERE T2.product_price > (SELECT avg(product_price) FROM products))')


result = cs.fetchall()
print(result)
