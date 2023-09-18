import csv
import logging
import os
import helper
import pyodbc

# get connection to the database
def connect_to_database(db_file):
    conn = None
    try:
        logging.info("Connecting to the database " + db_file)
        conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+db_file+';\'')
        logging.info("Connection established")
    except Exception as e:
        logging.error("Error while connecting to the database: " + str(e))
    return conn


def get_table_names_for_survey(conn, year, survey_name):
    cursor = conn.cursor()
    logging.info(f"Getting table names for {year} and {survey_name}")
    cursor.execute(f"select TableName from tables{year[-2:]} where Survey = '{survey_name}'")
    return [row[0] for row in cursor.fetchall()]

def extract_and_save_data(db_file, year, output_folder):
    conn = connect_to_database(db_file)
    if conn is None:
        return

    table_names = get_table_names_for_survey(conn, year, 'Institutional Characteristics')

    for table_name in table_names:
        cursor = conn.cursor()
        logging.info(f"Getting varname from vartable for {table_name}")
        cursor.execute(f"select varname from vartable{year[-2:]} where TableName = '{table_name}' and format = 'Disc'")
        varnames = cursor.fetchall()
        logging.info(f"Got varname from vartable for {table_name}")
        cursor.close()

        # update the values in column vartitle to codelabel from table valuesets where tablename = table_name and varname = varname and codevalue = table_name.varname
        cursor = conn.cursor()
        for varname in varnames:
            varname = varname[0]
            vartitle = varname + '_label'
            logging.info(f"Adding a new column {vartitle} for {table_name}")
            logging.info(f"alter table {table_name} add column {vartitle} VARCHAR 255")
            cursor.execute(f"alter table {table_name} add column {vartitle} VARCHAR 255")
            logging.info(f"Added a new column {vartitle} for {table_name}")

            logging.info(f"Updating the values in column {vartitle} for {table_name}")
            logging.info(f"UPDATE {table_name} INNER JOIN valuesets{year[-2:]} ON CSTR(valuesets{year[-2:]}.[codevalue]) = CSTR({table_name}.{varname}) SET {table_name}.{vartitle} = valuesets{year[-2:]}.valueLabel where valuesets{year[-2:]}.tablename = '{table_name}' and valuesets{year[-2:]}.varname = '{varname}'")
            cursor.execute(f"UPDATE {table_name} INNER JOIN valuesets{year[-2:]} ON CSTR(valuesets{year[-2:]}.[codevalue]) = CSTR({table_name}.{varname}) SET {table_name}.{vartitle} = valuesets{year[-2:]}.valueLabel where valuesets{year[-2:]}.tablename = '{table_name}' and valuesets{year[-2:]}.varname = '{varname}'")
            logging.info(f"Updated the values in column {vartitle} for {table_name}")
        cursor.close() 

        logging.info(f"Extracting data from {table_name} for {year}")
        cursor_inner = conn.cursor()
        cursor_inner.execute(f'select * from {table_name}')
        logging.info(f"Data extracted from {table_name} for {year}")

        file_name = ''.join([i for i in table_name if not i.isdigit()]) + '.csv'
        csv_path = os.path.join(output_folder, file_name)

        logging.info(f"Writing data to CSV file {file_name} for {year}")

        with open(csv_path, 'a', newline='') as f:
            csv_writer = csv.writer(f)
            for inner_row in cursor_inner.fetchall():
                inner_row = list(inner_row)
                inner_row.append(year)
                csv_writer.writerow(inner_row)

        logging.info(f"Data written to CSV file {file_name} for {year}")

        cursor_inner.close()

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
