#import the lib. and reference it as db
try:
    import psycopg2

    # print("✅ psycopg2 imported successfully!")

    # Test basic connection (replace with your details)
    conn = psycopg2.connect(
        host="localhost",
        database="dataengineering",
        user="postgres",
        password="postgres"
    )
    # print("✅ Database connection successful!")
    cur = conn.cursor()
    query = "insert into users(id,name,street,city,zip) values ({},'{}','{}','{}','{}')".format(1,'Big Bird','Seasame Street','Fakeville','12345')
    #Pass your query to the mogrify method:

    query2 = "insert into users(id,name,street,city,zip) values(%s,%s,%s,%s,%s)"
    data = (2,'Small Bird','Mustart Street','Mockville','6789')
    cur.mogrify(query)
    cur.mogrify(query2)

    #execute the query to insert the records

    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

except ImportError as e:
    print(f"❌ Import error: {e}")
except Exception as e:
    print(f"❌ Connection error: {e}")

#Create a connection string that contains the host,database, username, and password
# conn_string = "dbname='dataengineering' user='postgres' host='localhost' password='postgres'"
#
# #Create the connection object
# conn = db.connect(conn_string)
# cur = conn.cursor()
# cur.execute('SELECT 1')
