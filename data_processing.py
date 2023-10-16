import os
import re
import pandas as pd
import logging
import database_operations as db
import sqlalchemy as sa

logger = logging.getLogger(__name__)

def extract_and_save_data(db_file, year, create_csv, create_postgres_tables, output_folder, tables_to_merge, columnList={}):
    engine = db.connect_to_database(db_file)
    if engine is None:
        return
    try:
        survey_table_names = db.get_table_names(engine, year)
        postgres_engine = db.connect_to_ipeds_database()
        for survey_table_name in survey_table_names:
            survey = survey_table_name[0].replace(' ', '').split('(')[0]
            table_name = survey_table_name[1].upper()
            # remove more than 2 consecutive digits from the table name
            table_name_without_year = re.sub(r'\d{2,}', '', table_name)
            with engine.connect() as connection:
                logger.debug(f"Getting varname from vartable for {table_name}")
                query = f"select varname from vartable{year[-2:]} where TableName = '{table_name}' and format = 'Disc'"
                varnames = [row[0].upper().replace(' ', '') for row in connection.execute(sa.text(query))]
                logger.debug(f"Got varname from vartable for {table_name}")
                table_to_skip = None
                # Create a pandas df for the table along with the column names
                logger.debug(f"Reading data from {table_name}")
                if year in tables_to_merge['year'].values:
                    if table_name in tables_to_merge['table_to_skip'].values:
                        continue
                    elif table_name in tables_to_merge['table_to_merge_into'].values:
                        table_to_skip = tables_to_merge.loc[tables_to_merge['table_to_merge_into'] == table_name,
                                                           'table_to_skip'].iloc[0]
                        query = f"SELECT * from {table_name} inner join {table_to_skip} on {table_name}.UNITID = {table_to_skip}.UNITID"
                        df = pd.read_sql(query, connection)
                        # drop table_to_skip.UNITID to UNITID
                        df.rename(columns={table_to_skip + '.UNITID': 'UNITID'}, inplace=True)
                        df.rename(columns={table_name + '.UNITID': 'UNITID'}, inplace=True)
                        # remove duplicate columns
                        df = df.loc[:, ~df.columns.duplicated()]
                        query = f"select varname from vartable{year[-2:]} where TableName = '{table_to_skip}' and format = 'Disc'"
                        new_varnames = [row[0].upper().replace(' ', '') for row in connection.execute(sa.text(query))]
                        varnames.extend(new_varnames)
                    else:
                        query = f"SELECT * FROM {table_name}"
                        df = pd.read_sql(query, connection)
                else:
                    query = f"SELECT * FROM {table_name}"
                    df = pd.read_sql(query, connection)

                logger.debug(f"Created a pandas dataframe for {table_name}")

                # convert all the df column names to Upper case
                if not df.columns.str.isupper().all():
                    df.columns = df.columns.str.upper()

                for varname in varnames:
                    if table_to_skip is None:
                        query = f"select codevalue, valuelabel from valuesets{year[-2:]} where tablename = '{table_name}' and varname = '{varname}'"
                    else:
                        query = f"select codevalue, valuelabel from valuesets{year[-2:]} where tablename in ('{table_name}','{table_to_skip}') and varname = '{varname}'"
                    
                    value_mapping = {row[0]: row[1] for row in connection.execute(sa.text(query))}
                    logger.debug(f"Created a dictionary of codevalue and valuelabel for {varname} in {table_name}")
                    # get the column index of the varname
                    varname_index = df.columns.get_loc(varname)
                    # map the codevalue to valuelabel and insert the new column after the varname column
                    df.insert(varname_index + 1, varname + '_label', df[varname].astype(str).map(value_mapping))
                    logger.debug(f"Mapped the codevalue to valuelabel for {varname} in {table_name}")

                df.insert(0, 'Year', year)
                logger.debug(f"Added Year column to the dataframe for {year}")

                # Write the df to CSV file
                output_destination = output_folder.replace('<survey-name>', survey)
                file_name = table_name_without_year + '.csv'
                csv_path = os.path.join(output_destination, file_name)
                logger.debug(f"Writing data to CSV file {file_name} for {year}")

                new_columns = []
                # if df does not have any columns from columnList, then add the columns to the df with empty values
                if not all(elem in df.columns for elem in columnList[survey + '_' + table_name_without_year]):
                    for column in columnList[survey + '_' + table_name_without_year]:
                        if column not in df.columns:
                            new_columns.append(column)
                df = pd.concat([df, pd.DataFrame(columns=new_columns)], axis=1)
                if(create_csv):
                    df.to_csv(csv_path, index=False, header=False, mode='a',
                            columns=columnList[survey + '_' + table_name_without_year])
                    logger.info(f"Data written to CSV file {file_name} for {year}")
                if(create_postgres_tables):
                    # write the df data into postgres table
                    df.to_sql(survey + '_' + table_name_without_year, postgres_engine, if_exists='append', index=False)
                    logger.info(f"Data written to postgres table {survey + '_' + table_name_without_year} for {year}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        engine.dispose()
        logger.info(f"Connection closed after extracting data for {year}")
