import helper
import pyodbc


# read configurations.ini
config = helper.read_config()

# create logger
logger = helper.create_logger()

# get connection to the database
def create_connection(db_file):
    conn = None
    try:
        logger.info("Connecting to the database " + db_file)
        conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+db_file+';\'')
        logger.info("Connection established")
    except Exception as e:
        logger.error("Error while connecting to the database: " + str(e))
    return conn

#iterate throught the folder and get the access db file path
folderpath = config['Access DBs']['folderpath']
logger.info("Iterating through the folder: " + folderpath)
for file in helper.iterate_folder(folderpath, file_extension=".accdb"):
    logger.info("File found: " + file)
    # create a connection to the database with the path
    conn = create_connection(file)
    cursor = conn.cursor()
    logger.info("Reading data from the database " + file)
    year = file.split('\\')[-1].split('.')[0][-6:-2]
    cursor.execute('select TableName from tables'+ year[-2:] + " where Survey = 'Institutional Characteristics'")
    logger.info("Data read from the database " + file)

    for row in cursor.fetchall():
        curr = conn.cursor()
        curr.execute('select * from ' + row[0])
        file_name = ''.join([i for i in row[0] if not i.isdigit()]) + '.csv'
        csv_path = config['Access DBs']['PathToSaveCSV'] + '\\' + file_name
        logger.info("Writing data to csv file"+ file_name + " for " + year)
        with open(csv_path, 'a') as f:
            for row in curr.fetchall():
                row = list(row)
                row.append(year)
                f.write(str(row))
                f.write('\n')
        logger.info("Data written to csv file "+ file_name + " for " + year)
        curr.close()
    cursor.close()
    conn.close()
    logger.info("Connection closed")
