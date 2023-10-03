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
    
#create a sqlalchemy engine to connect to IPEDS postgres database
def connect_to_ipeds_database():
    engine = None
    try:
        logging.info("Connecting to the IPEDS postgres database")
        config = helper.read_config()
        ipeds_db = config['IPEDS DB']['dbname']
        ipeds_user = config['IPEDS DB']['user']
        ipeds_password = config['IPEDS DB']['password']
        ipeds_host = config['IPEDS DB']['host']
        ipeds_port = config['IPEDS DB']['port']
        connection_string = f"postgresql://{ipeds_user}:{ipeds_password}@{ipeds_host}:{ipeds_port}/{ipeds_db}"
        engine = sa.create_engine(connection_string)
        logging.info("IPEDS postgres database connection established")
    except Exception as e:
        logging.error("Error while connecting to the IPEDS postgres database: " + str(e))
    return engine    

def get_table_names(conn, year):
    cursor = conn.cursor()
    logging.info(f"Getting table names for {year}")
    cursor.execute(f"select Survey,TableName from tables{year[-2:]} where release <> 'NA'")
    return cursor.fetchall()

def get_table_columns(db_file, year, columns = {} ):
    conn = connect_to_database(db_file)
    if conn is None:
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
                    columns[survey+'_'+table_name_without_year].insert(1,'UNITID')
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

def extract_and_save_data(db_file, year, output_folder, tables_to_merge, columnList = {}):
    conn = connect_to_database(db_file)
    engine = get_sqlalchemy_engine(db_file)
    if conn is None or engine is None:
        return
    try:
        survey_table_names = get_table_names(conn, year)
        for survey_table_name in survey_table_names:
            survey = survey_table_name[0].replace(' ','')
            table_name = survey_table_name[1]
            # remove more than 2 consecutive digits from the table name
            table_name_without_year = re.sub('\d{2,}', '', table_name)            
            with conn.cursor() as cursor:
                logging.debug(f"Getting varname from vartable for {table_name}")
                cursor.execute(f"select varname from vartable{year[-2:]} where TableName = '{table_name}' and format = 'Disc'")
                varnames = [row[0].upper().replace(' ','') for row in cursor.fetchall()]
                logging.debug(f"Got varname from vartable for {table_name}")
                table_to_skip = None
                # Create a pandas df for the table along with the column names
                logging.debug(f"Reading data from {table_name}")
                if year in tables_to_merge['year'].values:    
                    if table_name in tables_to_merge['table_to_skip'].values:
                        continue
                    elif table_name in tables_to_merge['table_to_merge_into'].values:
                        table_to_skip = tables_to_merge.loc[tables_to_merge['table_to_merge_into'] == table_name, 'table_to_skip'].iloc[0]
                        df = pd.read_sql(f"SELECT* from {table_name} inner join {table_to_skip} on {table_name}.UNITID = {table_to_skip}.UNITID", engine)
                        # drop table_to_skip.UNITID to UNITID
                        df.rename(columns={table_to_skip+'.UNITID': 'UNITID'}, inplace=True)
                        df.rename(columns={table_name+'.UNITID': 'UNITID'}, inplace=True)
                        #remove duplicate columns
                        df = df.loc[:,~df.columns.duplicated()]
                        cursor.execute(f"select varname from vartable{year[-2:]} where TableName = '{table_to_skip}' and format = 'Disc'")
                        new_varnames = [row[0].upper().replace(' ','') for row in cursor.fetchall()]
                        varnames.extend(new_varnames)
                    else:
                        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
                else:
                    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

                logging.debug(f"Created a pandas dataframe for {table_name}")
                
                # convert all the df column names to Upper case
                if df.columns.str.isupper().all() == False:
                    df.columns = df.columns.str.upper()

                for varname in varnames:
                    if table_to_skip is None:
                        cursor.execute(f"select codevalue, valuelabel from valuesets{year[-2:]} where tablename = '{table_name}' and varname = '{varname}'")
                    else:
                        cursor.execute(f"select codevalue, valuelabel from valuesets{year[-2:]} where tablename in ('{table_name}','{table_to_skip}') and varname = '{varname}'")
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
            output_destination = output_folder.replace('<survey-name>', survey.split('(')[0])
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

def create_csv_files(output_folder, columnList = {}, tables_to_merge = pd.DataFrame()):
    for key in columnList:
        survey = key.split('_')[0].split('(')[0]
        table_name_without_year = key.split('_',1)[1]
        ls = []
        # create a list of tables to skip creating csv files
        for x in tables_to_merge['table_to_skip'].values:
            x = re.sub('\d{2,}', '', x)
            ls.append(x)
        
        if(table_name_without_year in ls):
            continue

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

def create_tables_in_postgres(columns):
    engine = connect_to_ipeds_database()
    if engine is None:
        return
    for key in columns:
        table_name = key.split('_',1)[1]
        df = pd.DataFrame(columns=columns[key])
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Created table {table_name}")


def main():
    config = helper.read_config()
    logging = helper.create_logger()

    accessdb_folderpath = config['Access DBs']['folderpath']
    csv_folderpath = config['Access DBs']['PathToSaveCSV']

    logging.info("Iterating through the folder: " + accessdb_folderpath)

    #move this to config file
    data = [['2008','EF2008F','EF2008D'], ['2008','EF2008DS','IC2008'], ['2010','CUSTOMCG2010','HD2010']]
    tables_to_merge = pd.DataFrame(data, columns=['year', 'table_to_skip', 'table_to_merge_into'])

    columns = {}
    for file in helper.iterate_folder(accessdb_folderpath, file_extension=".accdb"):
        logging.info("File found: " + file)
        year = file.split('\\')[-1].split('.')[0][-6:-2]
        
        # key = survey name_table name, value = list of columns
        columns = get_table_columns(file, year, columns)
    
    logging.info("Dictionary of columns for survey_table created")

    #create csv files with the column names
    create_csv_files(csv_folderpath, columns, tables_to_merge)
    logging.info("All CSV files created")

    for file in helper.iterate_folder(accessdb_folderpath, file_extension=".accdb"):
        logging.info("File found: " + file)
        year = file.split('\\')[-1].split('.')[0][-6:-2]
        extract_and_save_data(file, year, csv_folderpath, tables_to_merge, columnList = columns)
    logging.info("Data extracted and saved to CSV files")  
            
if __name__ == "__main__":
    main()
