import logging
import os
import re
import sqlalchemy as sa
import helper
import pandas as pd

logger = logging.getLogger(__name__)

def connect_to_database(db_file):
    try:
        logger.info("Connecting to the database using SQLAlchemy")
        connection_string = (
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            r"DBQ=" + db_file + ";"
            r"ExtendedAnsiSQL=1;")
        connection_url = sa.engine.URL.create("access+pyodbc", query={"odbc_connect": connection_string})
        engine = sa.create_engine(connection_url)
        logger.info("SQLAlchemy engine established")
        return engine
    except Exception as e:
        logger.error("Error while connecting to the database: " + str(e))
        return None

def connect_to_ipeds_database():
    try:
        logger.info("Connecting to the IPEDS postgres database")
        config = helper.read_config()
        ipeds_db = config['IPEDS DB']['dbname']
        ipeds_user = config['IPEDS DB']['user']
        ipeds_password = config['IPEDS DB']['password']
        ipeds_host = config['IPEDS DB']['host']
        ipeds_port = config['IPEDS DB']['port']
        connection_string = f"postgresql://{ipeds_user}:{ipeds_password}@{ipeds_host}:{ipeds_port}/{ipeds_db}"
        engine = sa.create_engine(connection_string)
        logger.info("IPEDS postgres database connection established")
        return engine
    except Exception as e:
        logger.error("Error while connecting to the IPEDS postgres database: " + str(e))
        return None

def get_table_names(engine, year):
    try:
        logger.info(f"Getting table names for {year}")
        connection = engine.connect()
        query = f"select Survey, TableName from tables{year[-2:]} where release <> 'NA'"
        result = connection.execute(sa.text(query))
        return result.fetchall()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return []
    finally:
        connection.close()

def get_table_columns(db_file, year, columns={}):
    engine = connect_to_database(db_file)
    if engine is None:
        return {}

    try:
        survey_table_names = get_table_names(engine, year)

        for survey_table_name in survey_table_names:
            survey = survey_table_name[0].replace(' ', '').split('(')[0]
            table_name = survey_table_name[1]
            table_name_without_year = re.sub('\d{2,}', '', table_name).replace(' ', '')

            with engine.connect() as connection:
                query = f"select varname from vartable{year[-2:]} where TableName = '{table_name}'"
                new_columns = connection.execute(sa.text(query)).fetchall()
                new_columns = [row[0].upper().replace(' ', '') for row in new_columns]
                new_columns.insert(0, 'Year')
                new_columns.insert(1, 'UNITID')

                if survey + '_' + table_name_without_year in columns:
                    seen = set(columns[survey + '_' + table_name_without_year])
                    list_of_new_columns = [x for x in new_columns if not (x in seen or seen.add(x))]
                    columns[survey + '_' + table_name_without_year].extend(list_of_new_columns)
                else:
                    columns[survey + '_' + table_name_without_year] = new_columns
                query = f"select varname from vartable{year[-2:]} where TableName = '{table_name}' and format = 'Disc'"
                categorical_varnames = connection.execute(sa.text(query)).fetchall()
                categorical_varnames = [row[0].upper().replace(' ', '') for row in categorical_varnames]
                for varname in categorical_varnames:
                    varname_index = columns[survey + '_' + table_name_without_year].index(varname)
                    if varname + '_label' not in columns[survey + '_' + table_name_without_year]:
                        columns[survey + '_' + table_name_without_year].insert(varname_index + 1, varname + '_label')

        return columns

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        engine.dispose()
        logger.info(f"Connection closed after getting table columns {year}")

def create_tables(table_columns, tables_to_merge):
    postgres_engine = connect_to_ipeds_database()
    # Create PostgreSQL tables with the final structure
    for key in table_columns:
        survey = key.split('_')[0].split('(')[0]
        table_name_without_year = key.split('_',1)[1].upper()
        ls = []
        # create a list of tables to skip creating csv files
        for x in tables_to_merge['table_to_skip'].values:
            x = re.sub('\d{2,}', '', x)
            ls.append(x)
        
        if(table_name_without_year in ls):
            continue

        columns = [sa.Column(col_name, sa.Text) for col_name in table_columns[key]]

        # Create a new table in PostgreSQL with the final structure
        new_table = sa.Table(
            survey+'_'+table_name_without_year,
            sa.MetaData(),
            *columns,
        )

        # Create the table in PostgreSQL (if it doesn't exist)
        new_table.create(postgres_engine, checkfirst=True)
    
    # Close the PostgreSQL database connection
    postgres_engine.dispose()

def load_data_into_postgres_from_csv(output_folder,table_columns):
    try:
        # Establish a connection to the PostgreSQL database
        engine = connect_to_ipeds_database()
        connection = engine.connect()
        cursor = connection.connection.cursor()
        for key in table_columns:
            survey = key.split('_')[0].split('(')[0]
            table_name_without_year = key.split('_',1)[1].upper()
            
            output_destination = output_folder.replace('<survey-name>', survey)
            file_name = table_name_without_year + '.csv'
            csv_path = os.path.join(output_destination, file_name)
        
            with open(csv_path) as csvFile:
                next(csvFile)  # SKIP HEADERS
                cursor.copy_from(csvFile, f'{survey}_{table_name_without_year}', sep=",")

            print(f"Data from {csv_path} copied to {survey+'_'+table_name_without_year} successfully.")
        connection.close()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection:
            connection.close()