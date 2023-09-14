import csv
import os
import helper
import pyodbc

# get connection to the database
def connect_to_database(db_file, logger):
    conn = None
    try:
        logger.info("Connecting to the database " + db_file)
        conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+db_file+';\'')
        logger.info("Connection established")
    except Exception as e:
        logger.error("Error while connecting to the database: " + str(e))
    return conn


def get_table_names_for_survey(conn, year, survey_name, logger):
    cursor = conn.cursor()
    logger.info(f"Getting table names for {year} and {survey_name}")
    cursor.execute(f"select TableName from tables{year[-2:]} where Survey = '{survey_name}'")
    return [row[0] for row in cursor.fetchall()]

def extract_and_save_data(db_file, year, output_folder, logger):
    conn = connect_to_database(db_file, logger)
    if conn is None:
        return

    table_names = get_table_names_for_survey(conn, year, 'Institutional Characteristics', logger)

    for table_name in table_names:
        # get varname, vartitle from the table vartable with tablename = table_name and format = disc
        cursor = conn.cursor()
        logger.info(f"Getting varname, vartitle from vartable for {table_name}")
        cursor.execute(f"select varname, vartitle, DataType from vartable{year[-2:]} where TableName = '{table_name}' and format = 'Disc'")
        varname_vartitle = cursor.fetchall()
        logger.info(f"Got varname, vartitle from vartable for {table_name}")
        cursor.close()

        # update the values in column vartitle to codelabel from table valuesets where tablename = table_name and varname = varname and codevalue = table_name.varname
        cursor = conn.cursor()
        for varname, vartitle, datatype in varname_vartitle:
            #vartitle = vartitle.replace(' ', '_').replace('-','_')[:25]
            vartitle = varname + '_label'
            logger.info(f"Adding a new column {vartitle} for {table_name}")
            cursor.execute(f"alter table {table_name} add column {vartitle} VARCHAR 255")
            logger.info(f"Added a new column {vartitle} for {table_name}")
            logger.info(f"Updating the values in column {vartitle} for {table_name}")
            if datatype == 'N':
                logger.info(f"UPDATE {table_name} INNER JOIN valuesets{year[-2:]} ON valuesets{year[-2:]}.codevalue = CAST({table_name}.{varname} AS VARCHAR 25) SET {table_name}.{vartitle} = valuesets{year[-2:]}.valueLabel where valuesets{year[-2:]}.tablename = '{table_name}' and valuesets{year[-2:]}.varname = '{varname}'")
                #cursor.execute(f"UPDATE {table_name} INNER JOIN valuesets{year[-2:]} ON valuesets{year[-2:]}.codevalue = CAST({table_name}.{varname} AS VARCHAR 25)  SET {table_name}.{vartitle} = valuesets{year[-2:]}.valueLabel where valuesets{year[-2:]}.tablename = '{table_name}' and valuesets{year[-2:]}.varname = '{varname}'")
            elif datatype == 'A':
                logger.info(f"UPDATE {table_name} INNER JOIN valuesets{year[-2:]} ON valuesets{year[-2:]}.codevalue = {table_name}.{varname} SET {table_name}.{vartitle} = valuesets{year[-2:]}.valueLabel where valuesets{year[-2:]}.tablename = '{table_name}' and valuesets{year[-2:]}.varname = '{varname}'")
                cursor.execute(f"UPDATE {table_name} INNER JOIN valuesets{year[-2:]} ON valuesets{year[-2:]}.codevalue = {table_name}.{varname} SET {table_name}.{vartitle} = valuesets{year[-2:]}.valueLabel where valuesets{year[-2:]}.tablename = '{table_name}' and valuesets{year[-2:]}.varname = '{varname}'")
            # logger.info(f"update {table_name} set {vartitle} = (select valueLabel from valuesets{year[-2:]} where tablename = '{table_name}' and varname = '{varname}' and codevalue = {table_name}.{varname})")
            # cursor.execute(f"update {table_name} set {vartitle} = (select valueLabel from valuesets{year[-2:]} where tablename = '{table_name}' and varname = '{varname}' and codevalue = {table_name}.{varname})")
            logger.info(f"Updated the values in column {vartitle} for {table_name}")
        cursor.close() 

        logger.info(f"Extracting data from {table_name} for {year}")
        cursor_inner = conn.cursor()
        cursor_inner.execute(f'select * from {table_name}')
        logger.info(f"Data extracted from {table_name} for {year}")

        file_name = ''.join([i for i in table_name if not i.isdigit()]) + '.csv'
        csv_path = os.path.join(output_folder, file_name)

        logger.info(f"Writing data to CSV file {file_name} for {year}")

        with open(csv_path, 'a', newline='') as f:
            csv_writer = csv.writer(f)
            for inner_row in cursor_inner.fetchall():
                inner_row = list(inner_row)
                inner_row.append(year)
                csv_writer.writerow(inner_row)

        logger.info(f"Data written to CSV file {file_name} for {year}")

        cursor_inner.close()

    conn.close()
    logger.info("Connection closed")

def main():
    # read configurations.ini
    config = helper.read_config()

    # create logger
    logger = helper.create_logger()

    folderpath = config['Access DBs']['folderpath']

    logger.info("Iterating through the folder: " + folderpath)

    for file in helper.iterate_folder(folderpath, file_extension=".accdb"):
        logger.info("File found: " + file)
        year = file.split('\\')[-1].split('.')[0][-6:-2]
        extract_and_save_data(file, year, config['Access DBs']['PathToSaveCSV'], logger)

if __name__ == "__main__":
    main()
