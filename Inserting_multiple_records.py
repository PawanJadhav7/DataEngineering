from faker import Faker
from Postgres_Python_Connection import createconnection, insert_query

fake = Faker()
data = []
i=3
for _ in range(1000):
    data.append((i,
        fake.name(),
        fake.street_address(),
        fake.city(),
        fake.zipcode()
    ))
    i+=1
# Convert list of tuples to tuple of tuples
data_for_db = tuple(data)
# print(data_for_db)
insert_query(data_for_db)