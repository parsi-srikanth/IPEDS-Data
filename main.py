import pyodbc
from pyodbc import Error

# get connection to the database
def create_connection(db_file):
    conn = None
    try:
        # use the parameters to connect to the database
        conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+db_file+';\'')
    except Error as e:
        print(e)
    return conn

# create a connection to the database with the path
conn = create_connection(r"C:\Users\C00541311\Desktop\IPEDS202122\IPEDS202122.accdb")
cursor = conn.cursor()
cursor.execute('select top 1 * from tables21')
   
for row in cursor.fetchall():
    print (row)

conn.close()