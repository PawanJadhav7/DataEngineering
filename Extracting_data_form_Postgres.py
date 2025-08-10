from Postgres_Python_Connection import createconnection, select_query
import psycopg2 as db
import pandas as pd

users = select_query()
# for user in users:
#     print(user["name"])

df = pd.read_sql("select * from users", con=createconnection())
df.to_json("users.json",orient='records',indent=4)
