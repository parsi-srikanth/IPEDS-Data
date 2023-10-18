from data_processing import extract_and_save_data
from database_operations import get_counts, get_table_columns
from file_operations import create_csv_files, iterate_folder
import helper
import pandas as pd
from warnings import simplefilter 
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

def main():
    config = helper.read_config()
    logging = helper.create_logger()
    logger = logging.getLogger(__name__)
    accessdb_folderpath = config['Access DBs']['folderpath']
    csv_folderpath = config['Access DBs']['PathToSaveCSV']
    create_csv = True if config['Output']['CreateCsv'] == 'True' else False
    create_postgres_tables = True if config['Output']['CreatePostgresTable'] == 'True' else False

    logger.info("Iterating through the folder: " + accessdb_folderpath)

    #move this to config file
    data = [['2008','EF2008F','EF2008D'], ['2008','EF2008DS','IC2008'], ['2010','CUSTOMCG2010','HD2010']]
    tables_to_merge = pd.DataFrame(data, columns=['year', 'table_to_skip', 'table_to_merge_into'])

    columns = {}
    for file in iterate_folder(accessdb_folderpath, file_extension=".accdb"):
        logger.info("File found: " + file)
        year = file.split('\\')[-1].split('.')[0][-6:-2]      
        # key = survey name_table name, value = list of columns
        columns = get_table_columns(file, year, columns)
    logger.info("Dictionary of columns for survey_table created")

    #add columns that are not in the vartable
    columns['InstitutionalCharacteristics_DRVIC'].append('DVIC13')
    columns['HumanResources_DRVHR'].append('ACT')
    columns['InstitutionalCharacteristics_IC_PY'].append('CIPTITLE1')
    columns['HumanResources_SAL_FACULTY'].append('I')
    columns['HumanResources_SAL_FACULTY'].append('DROP')

    #create csv files with the column names
    if(create_csv):
        create_csv_files(csv_folderpath, columns, tables_to_merge)
        logger.info("All CSV files created")

    for file in iterate_folder(accessdb_folderpath, file_extension=".accdb"):
        logger.info("File found: " + file)
        year = file.split('\\')[-1].split('.')[0][-6:-2]
        extract_and_save_data(file, year, create_csv, create_postgres_tables, csv_folderpath, tables_to_merge, columnList = columns)
    logger.info("Data extracted and saved")  

    counts = get_counts(columns)
    counts.to_csv('counts.csv', index=False)
    logger.info("Counts saved to CSV file")
if __name__ == "__main__":
    main()
