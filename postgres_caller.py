from postgres_manager import DBManager, usage

connection = {
    'database': 'just_test',  # database name
    'user': 'graham',  # username
    'password': '7112004gW!',  # password
    'host': 'localhost',  # host name
    'port': '5432'  # port number
}
table = {'table_sql': None}
select = "SELECT * FROM employee"
insert = "INSERT INTO employee(name, state) VALUES('Shayla', 'Great')"

y = DBManager(connection)

y.connect()
s = y.select(select)
y.insert(insert)

y.disconnect()
print(s)
