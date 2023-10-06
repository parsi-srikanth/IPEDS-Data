import logging
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
            survey = survey_table_name[0].replace(' ', '')
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

def create_tables_in_postgres(columns):
    engine = connect_to_ipeds_database()
    if engine is None:
        return
    
    for key in columns:
        table_name = key.split('_', 1)[1]
        df = pd.DataFrame(columns=columns[key])
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"Created table {table_name}")

def get_columns(filepath, year, table_columns):
    # Connect to the Access database
    access_engine = connect_to_database(filepath)
    access_conn = access_engine.connect()

    # Reflect tables from the Access database
    metadata = sa.MetaData()
    metadata.reflect(bind=access_engine)

    # Iterate through the reflected tables
    for table_name, table in metadata.tables.items():
        query = f"select Survey from tables{year[-2:]} where TableName = '{table_name}'"
        Survey = access_conn.execute(sa.text(query))
        Survey = Survey.first()
        if Survey == None or Survey == 'NA':
            continue
        table_name_without_year = re.sub('\d{2,}', '', table_name).replace(' ', '')
        # Track the columns for each table
        if table_columns is not None and table_name_without_year not in table_columns:
            table_columns[table_name_without_year] = set()

        # Add the columns to the set for this year
        table_columns[table_name_without_year].update([column.name for column in table.columns])

    # Close the Access database connection
    access_conn.close()
    return table_columns

def create_tables(table_columns):
    postgres_engine = connect_to_ipeds_database()
    # Create PostgreSQL tables with the final structure
    for table_name, columns in table_columns.items():
        # Convert column names to Column objects
        columns = [sa.Column(col_name, sa.String) for col_name in columns]

        # Create a new table in PostgreSQL with the final structure
        new_table = sa.Table(
            table_name,
            sa.MetaData(),
            *columns,
            sa.Column('year', sa.Integer),  # Add a 'year' column of type Integer
        )

        # Create the table in PostgreSQL (if it doesn't exist)
        new_table.create(postgres_engine, checkfirst=True)
    
    # Close the PostgreSQL database connection
    postgres_engine.dispose()