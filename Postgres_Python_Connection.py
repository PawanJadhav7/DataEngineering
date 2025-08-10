#import the lib. and reference it as db
import psycopg2
from psycopg2.extras import RealDictCursor


def createconnection():
    try:
        # Test basic connection (replace with your details)
        conn = psycopg2.connect(
            host="localhost",
        database="dataengineering",
            user="postgres",
            password="postgres"
        )
        return conn
    except Exception as e:
        print(f"❌ Connection error: {e}")

def insert_query(data:tuple):
    try:
        conn = createconnection()
        if conn is None:
            return None
        cur = conn.cursor()

        # Pass your query to the mogrify method:
        insert_query = "insert into users(id,name,street,city,zip) values(%s,%s,%s,%s,%s)"

        # execute the query to insert the records
        cur.executemany(insert_query, data)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return None
def select_query():
    try:
        conn = createconnection()
        if conn is None:
            return None
        cur = conn.cursor()
        cur.execute("SELECT * FROM users")
        results = cur.fetchall()
        cur.close()
        conn.close()
        return results
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return None