import logging
import os
import re
import sqlalchemy as sa
import helper
import pyodbc
import pandas as pd
from warnings import simplefilter 
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
simplefilter(action='ignore', category=FutureWarning)

# get connection to the database
def connect_to_database(db_file):
    conn = None
    try:
        logging.info("Connecting to the database pyodbc ")
        conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+db_file+';\'')
        logging.info("Connection established")
    except Exception as e:
        logging.error("Error while connecting to the database: " + str(e))
    return conn

def get_sqlalchemy_engine(db_file):
    engine = None
    try:
        logging.info("Connecting to the database sqlalchemy ")
        connection_string = (
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            r"DBQ="+db_file+";"
            r"ExtendedAnsiSQL=1;")
        connection_url = sa.engine.URL.create("access+pyodbc",query={"odbc_connect": connection_string})
        engine = sa.create_engine(connection_url)
        logging.info("sqlalchemy engine established")
    except Exception as e:
        logging.error("Error while creating sqlalchemy engine: " + str(e))
    return engine
    

def get_table_names(conn, year):
    cursor = conn.cursor()
    logging.info(f"Getting table names for {year}")
    cursor.execute(f"select Survey,TableName from tables{year[-2:]} where release <> 'NA'")
    return cursor.fetchall()

def extract_and_save_data(db_file, year, output_folder):
    conn = connect_to_database(db_file)
    engine = get_sqlalchemy_engine(db_file)
    if conn is None or engine is None:
        return
    try:
        survey_table_names = get_table_names(conn, year)
        for survey_table_name in survey_table_names:
            survey = survey_table_name[0]
            table_name = survey_table_name[1]
            with conn.cursor() as cursor:
                logging.debug(f"Getting varname from vartable for {table_name}")
                cursor.execute(f"select varname from vartable{year[-2:]} where TableName = '{table_name}' and format = 'Disc'")
                varnames = [row[0].upper() for row in cursor.fetchall()]
                logging.debug(f"Got varname from vartable for {table_name}")

                # Create a pandas df for the table along with the column names
                df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
                logging.info(f"Created a pandas dataframe for {table_name}")
                
                # remove more than 2 consecutive digits from the table name
                table_name_without_year = re.sub('\d{2,}', '', table_name)
                
                # convert all the df column names to Upper case
                if df.columns.str.isupper().all() == False:
                    df.columns = df.columns.str.upper()

                for varname in varnames:
                    cursor.execute(f"select codevalue, valuelabel from valuesets{year[-2:]} where tablename = '{table_name}' and varname = '{varname}'")
                    value_mapping = dict(cursor.fetchall())
                    logging.debug(f"Created a dictionary of codevalue and valuelabel for {varname} in {table_name}")
                    # get the column index of the varname
                    varname_index = df.columns.get_loc(varname)
                    # map the codevalue to valuelabel and insert the new column after the varname column
                    df.insert(varname_index+1,varname+'_label', df[varname].astype(str).map(value_mapping))
                    logging.debug(f"Mapped the codevalue to valuelabel for {varname} in {table_name}")

            # Write the df to CSV file
            output_destination = output_folder.replace('survey', survey)
            os.makedirs(output_destination, exist_ok=True)
            file_name = table_name_without_year + '.csv'
            csv_path = os.path.join(output_destination, file_name)
            logging.debug(f"Writing data to CSV file {file_name} for {year}")
            df.insert(0,'Year', year)
            #df.to_csv(csv_path,mode='a')
            if not os.path.exists(csv_path):
                df.to_csv(csv_path, index=False, header=True)
            else:
                #read whole csv file as chunks of 1000 rows at a time
                existing_df = None
                with pd.read_csv(csv_path, chunksize=1000) as reader:
                    for chunk in reader:
                        if existing_df is None:
                            existing_df = chunk
                        else:
                            existing_df = pd.concat([existing_df, chunk])
                #append the new df to existing df
                appended_df = pd.concat([existing_df, df])
                appended_df.to_csv(csv_path, index=False, header=True)
            logging.info(f"Data written to CSV file {file_name} for {year}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        conn.close()
        logging.info("Connection closed")

def main():
    # read configurations.ini
    config = helper.read_config()

    # create logging
    logging = helper.create_logger()

    folderpath = config['Access DBs']['folderpath']

    logging.info("Iterating through the folder: " + folderpath)

    for file in helper.iterate_folder(folderpath, file_extension=".accdb"):
        logging.info("File found: " + file)
        year = file.split('\\')[-1].split('.')[0][-6:-2]
        extract_and_save_data(file, year, config['Access DBs']['PathToSaveCSV'])

if __name__ == "__main__":
    main()
