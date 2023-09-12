import helper
import logging
import pyodbc
from pyodbc import Error

# read configurations.ini
config = helper.read_config()

# create logger
logger = helper.create_logger()

# get connection to the database
def create_connection(db_file):
    conn = None
    try:
        logger.info("Connecting to the database")
        conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+db_file+';\'')
        logger.info("Connection established")
    except Error as e:
        logger.error("Error while connecting to the database: " + str(e))
    return conn

# create a connection to the database with the path
conn = create_connection(config['Access DB File Path']['IPEDS202122'])
cursor = conn.cursor()
logger.info("Reading data from the database")
cursor.execute('select top 1 * from tables21')
logger.info("Data read from the database")

for row in cursor.fetchall():
    print (row)

conn.close()
logger.info("Connection closed")