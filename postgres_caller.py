from postgres_manager import DBManager, usage

connection = {
    'database': 'just_bad',  # database name
    'user': 'graham',  # username
    'password': '7112004gW!',  # password
    'host': 'localhost',  # host name
    'port': '5432'  # port number
}
table = "CREATE TABLE employee(name VARCHAR(20), state VARCHAR(20))"
select = "SELECT * FROM employee"
insert1 = "INSERT INTO employee(name, state) VALUES('Dan', 'Okay')"
insert2 = "INSERT INTO employee(name, state) VALUES('Steve', 'Meh')"
drop = "DROP TABLE employee"

y = DBManager(connection, True)

y.connect()

# lst = y.select(select)

# y.create(table)
# y.insert(insert1)
# y.insert(insert2)
# y.commit()
lst = y.select(select)

# y.drop_table(drop)
# y.commit()
y.disconnect()
print(lst)



