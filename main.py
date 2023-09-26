import csv
import logging
import os
import re
import sqlalchemy as sa
import helper
import pyodbc
import pandas as pd
from warnings import simplefilter 
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
# simplefilter(action='ignore', category=FutureWarning)

# get connection to the database
def connect_to_database(db_file):
    conn = None
    try:
        logging.info("Connecting to the database pyodbc ")
        conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+db_file+';\'')
        logging.info("Access DB Connection established")
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
    cursor.execute(f"select Survey,TableName from tables{year[-2:]} where release <> 'NA' and survey = 'Completions'")
    return cursor.fetchall()

def get_table_columns(db_file, year, columns = {} ):
    conn = connect_to_database(db_file)
    engine = get_sqlalchemy_engine(db_file)
    if conn is None or engine is None:
        return

    try:
        survey_table_names = get_table_names(conn, year)
        for survey_table_name in survey_table_names:
            survey = survey_table_name[0].replace(' ','')
            table_name = survey_table_name[1]
            table_name_without_year = re.sub('\d{2,}', '', table_name).replace(' ','')
            with conn.cursor() as cursor:
                cursor.execute(f"select varname from vartable{year[-2:]} where TableName = '{table_name}'")
                # add the columns as a list to the dictionary with key as survey name and table name without year and digits in the table name as value 
                # add only if the columns are not already present in the dictionary
                if survey+'_'+table_name_without_year in columns:
                    seen = set(columns[survey+'_'+table_name_without_year])
                    list_of_new_columns = (x for x in [row[0].upper().replace(' ','') for row in cursor.fetchall()] if not (x in seen or seen.add(x)))
                    columns[survey+'_'+table_name_without_year].extend(list_of_new_columns)
                else:
                    columns[survey+'_'+table_name_without_year] = list([row[0].upper().replace(' ','') for row in cursor.fetchall()])
                    columns[survey+'_'+table_name_without_year].insert(0,'Year')
                cursor.execute(f"select varname from vartable{year[-2:]} where TableName = '{table_name}' and format = 'Disc'")
                categorical_varnames = [row[0].upper().replace(' ','') for row in cursor.fetchall()]
                for varname in categorical_varnames:
                    # get the column index of the varname
                    varname_index = columns[survey+'_'+table_name_without_year].index(varname)
                    # insert the new column after the varname column
                    if varname+'_label' not in columns[survey+'_'+table_name_without_year]:
                        columns[survey+'_'+table_name_without_year].insert(varname_index+1,varname+'_label')
        return columns

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        conn.close()
        logging.info(f"Connection closed after getting table columns {year}")

def extract_and_save_data(db_file, year, output_folder, columnList = {}):
    conn = connect_to_database(db_file)
    engine = get_sqlalchemy_engine(db_file)
    if conn is None or engine is None:
        return
    try:
        survey_table_names = get_table_names(conn, year)
        for survey_table_name in survey_table_names:
            survey = survey_table_name[0].replace(' ','')
            table_name = survey_table_name[1]
            with conn.cursor() as cursor:
                logging.debug(f"Getting varname from vartable for {table_name}")
                cursor.execute(f"select varname from vartable{year[-2:]} where TableName = '{table_name}' and format = 'Disc'")
                varnames = [row[0].upper().replace(' ','') for row in cursor.fetchall()]
                logging.debug(f"Got varname from vartable for {table_name}")

                # Create a pandas df for the table along with the column names
                logging.debug(f"Reading data from {table_name}")
                df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
                logging.debug(f"Created a pandas dataframe for {table_name}")
                
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

            df.insert(0,'Year', year)
            logging.debug(f"Added Year column to the dataframe for {year}")

            # Write the df to CSV file
            output_destination = output_folder.replace('<survey-name>', survey)
            file_name = table_name_without_year + '.csv'
            csv_path = os.path.join(output_destination, file_name)
            logging.debug(f"Writing data to CSV file {file_name} for {year}")

            new_columns = []
            # if df is not have any columns from columnList, then add the columns to the df with empty values
            if not all(elem in df.columns for elem in columnList[survey+'_'+table_name_without_year]):
                for column in columnList[survey+'_'+table_name_without_year]:
                    if column not in df.columns:
                        new_columns.append(column)
            df = pd.concat([df, pd.DataFrame(columns=new_columns)], axis=1)
            df.to_csv(csv_path, index=False, header=False, mode='a', columns=columnList[survey+'_'+table_name_without_year])
            logging.info(f"Data written to CSV file {file_name} for {year}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        conn.close()
        logging.info(f"Connection closed after extracting data for {year}")

def create_csv_files(output_folder, columnList = {}):
    for key in columnList:
        survey = key.split('_')[0]
        table_name_without_year = key.split('_',1)[1]
        
        # Write the df to CSV file
        output_destination = output_folder.replace('<survey-name>', survey)
        os.makedirs(output_destination, exist_ok=True)
        file_name = table_name_without_year + '.csv'
        csv_path = os.path.join(output_destination, file_name)
        
        #create a csv file with the columnList as the header
        if not os.path.isfile(csv_path):
            with open(csv_path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(columnList[key])
        logging.info(f"Created CSV file {file_name}") 

def main():
    config = helper.read_config()
    logging = helper.create_logger()

    accessdb_folderpath = config['Access DBs']['folderpath']
    csv_folderpath = config['Access DBs']['PathToSaveCSV']

    logging.info("Iterating through the folder: " + accessdb_folderpath)

    columns = {}
    for file in helper.iterate_folder(accessdb_folderpath, file_extension=".accdb"):
        logging.info("File found: " + file)
        year = file.split('\\')[-1].split('.')[0][-6:-2]
        
        # key = survey name_table name, value = list of columns
        columns = get_table_columns(file, year, columns)
    
    logging.info("Dictionary of columns for survey_table created")

    #create csv files with the column names
    create_csv_files(csv_folderpath, columns)
    logging.info("All CSV files created")

    for file in helper.iterate_folder(accessdb_folderpath, file_extension=".accdb"):
        logging.info("File found: " + file)
        year = file.split('\\')[-1].split('.')[0][-6:-2]

        extract_and_save_data(file, year, csv_folderpath, columnList = columns)
    logging.info("Data extracted and saved to CSV files")  
            
if __name__ == "__main__":
    main()
