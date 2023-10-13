import logging
import re
import pandas as pd
from sqlalchemy import inspect
import sqlalchemy as sa

from database_operations import connect_to_database, connect_to_ipeds_database

logger = logging.getLogger(__name__)
def connect_and_reflect_access_dbs(db_path, year, existing_df=None):
    """
    Connect to Access databases, reflect tables, and retrieve column information.

    Args:
        db_paths :  file path to Access databases.
        existing_df (pd.DataFrame, optional): Existing DataFrame to append to. Default is None.

    Returns:
        pd.DataFrame: DataFrame containing table and column information.
    """
    # Connect to the Access database
    access_engine = connect_to_database(db_path)
    conn = access_engine.connect()
    
    # Initialize an empty DataFrame or use an existing one
    if existing_df is None:
        combined_df = pd.DataFrame()
    else:
        combined_df = existing_df


    # Reflect the tables in the database
    inspector = inspect(conn)
    table_names = inspector.get_table_names()
    # Iterate through the tables
    for table_name in table_names:
        # Get the table's column information
        query = f"select Survey from tables{year[-2:]} where TableName = '{table_name}'"
        Survey = conn.execute(sa.text(query))
        Survey = Survey.first()
        if Survey == None or Survey == 'NA':
            continue
        table_name_without_year = re.sub('\d{2,}', '', table_name).replace(' ', '')
        columns = inspector.get_columns(table_name)
        # Create a DataFrame for the current table's columns
        table_df = pd.DataFrame(columns)
        table_df['Table'] = table_name_without_year  # Add a 'Table' column
        # Check if the table already exists in the combined DataFrame
        if 'Table' in combined_df.columns and table_name_without_year in combined_df['Table'].values:
            # Update column widths if necessary
            combined_df = update_column_widths(combined_df, table_df)
        else:
            # Append the table DataFrame to the combined DataFrame
            combined_df = pd.concat([combined_df, table_df], ignore_index=True)
    # Close the database connection
    conn.close()

    return combined_df

def update_column_widths(existing_df, new_df):
    """
    Update column widths in an existing DataFrame based on a new DataFrame.

    Args:
        existing_df (pd.DataFrame): Existing DataFrame.
        new_df (pd.DataFrame): New DataFrame with potentially updated column widths.

    Returns:
        pd.DataFrame: Updated DataFrame with column widths adjusted.
    """
    for col in new_df.columns:
        # Check if the width column exists in both DataFrames
        if col + '_width' in existing_df.columns and col + '_width' in new_df.columns:
            existing_col_width = existing_df.loc[existing_df['Table'] == new_df['Table'][0], col + '_width'].values[0]
            new_col_width = new_df.loc[0, col + '_width']
            combined_width = max(existing_col_width, new_col_width)
            existing_df.loc[existing_df['Table'] == new_df['Table'][0], col + '_width'] = combined_width
    return existing_df

# Mapping between Access data types and PostgreSQL data types
data_type_mapping = {
    'Boolean': 'BOOLEAN',
    'Byte': 'INTEGER',
    'INTEGER': 'INTEGER',
    'Long Integer': 'INTEGER',
    'TINYINT': 'INTEGER',
    'SMALLINT': 'INTEGER',
    'Date/Time': 'TIMESTAMP',
    'Currency': 'Numeric',
    'Single': 'Numeric',
    'Double': 'Numeric',
    'Decimal': 'Numeric',
    # Add more data type mappings as needed
}

def map_access_to_postgres_data_type(access_data_type):
    """
    Map Access data types to PostgreSQL data types.

    Args:
        access_data_type (str): Access data type.

    Returns:
        str: Corresponding PostgreSQL data type.
    """
    return data_type_mapping.get(access_data_type, sa.text)  # Default to 'text' if no mapping found

def create_postgres_tables(df):
    """
    Create PostgreSQL tables based on DataFrame information.

    Args:
        df (pd.DataFrame): DataFrame containing table and column information.

    Returns:
        None
    """
    # Create a SQLAlchemy engine for PostgreSQL
    engine = connect_to_ipeds_database()

    # Group the DataFrame by 'Table' to create tables
    table_groups = df.groupby('Table')

    # Iterate through each group (table)
    for table_name, group_df in table_groups:
        # Extract column names and Access data types from the DataFrame
        columns = group_df['name'].tolist()
        access_data_types = group_df['type'].tolist()

        # Map Access data types to PostgreSQL data types
        postgres_data_types = [map_access_to_postgres_data_type(data_type) for data_type in access_data_types]

        # # Generate the CREATE TABLE SQL statement
        # create_table_sql = f"CREATE TABLE {table_name} ("
        # for col, col_type in zip(columns, postgres_data_types):
        #     create_table_sql += f"{col} {col_type}, "
        # create_table_sql = create_table_sql.rstrip(', ') + ");"
        # logger.info(f"SQL statement for table {table_name}: {create_table_sql}")
        # # Execute the SQL statement to create the table
        # with engine.connect() as connection:
        #     connection.execute(sa.text(create_table_sql))

        # create column objects
        columns = [sa.Column(col_name, col_type) for col_name, col_type in zip(columns, postgres_data_types)]

        # Create a new table in PostgreSQL with the final structure
        new_table = sa.Table(
            table_name,
            sa.MetaData(),
            *columns,
            sa.Column('year', sa.Integer),  # Add a 'year' column of type Integer
        )
        # Create the table in PostgreSQL (if it doesn't exist)
        new_table.create(engine, checkfirst=True)
    
    # Close the PostgreSQL database connection
    engine.dispose()



def create_tables(table_columns, tables_to_merge):
    postgres_engine = connect_to_ipeds_database()
    # Create PostgreSQL tables with the final structure
    for key in table_columns:
        survey = key.split('_')[0].split('(')[0]
        table_name_without_year = key.split('_',1)[1].upper()
        skip_tables = tables_to_merge['table_to_skip'].str.replace(r'\d{2,}', '', regex=True)

        if table_name_without_year in skip_tables.values:
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

# def load_data_into_postgres_from_csv(output_folder,table_columns):
#     try:
#         # Establish a connection to the PostgreSQL database
#         engine = connect_to_ipeds_database()
#         connection = engine.connect()
#         cursor = connection.connection.cursor()
#         for key in table_columns:
#             survey = key.split('_')[0].split('(')[0]
#             table_name_without_year = key.split('_',1)[1].upper()
            
#             output_destination = output_folder.replace('<survey-name>', survey)
#             file_name = table_name_without_year + '.csv'
#             csv_path = os.path.join(output_destination, file_name)
        
#             with open(csv_path) as csvFile:
#                 next(csvFile)  # SKIP HEADERS
#                 cursor.copy_from(csvFile, f'{survey}_{table_name_without_year}', sep=",")

#             print(f"Data from {csv_path} copied to {survey+'_'+table_name_without_year} successfully.")
#         connection.close()
#     except Exception as e:
#         print(f"Error: {e}")
#     finally:
#         if connection:
#             connection.close()