from data_processing import extract_and_save_data
from database_operations import create_tables, get_columns, get_table_columns
from file_operations import create_csv_files
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

    logger.info("Iterating through the folder: " + accessdb_folderpath)

    #move this to config file
    data = [['2008','EF2008F','EF2008D'], ['2008','EF2008DS','IC2008'], ['2010','CUSTOMCG2010','HD2010']]
    tables_to_merge = pd.DataFrame(data, columns=['year', 'table_to_skip', 'table_to_merge_into'])

    columns = {}
    table_columns = {}
    for file in helper.iterate_folder(accessdb_folderpath, file_extension=".accdb"):
        logger.info("File found: " + file)
        year = file.split('\\')[-1].split('.')[0][-6:-2]
        
        # key = survey name_table name, value = list of columns
        if(config['Output']['CreateCsv'].__eq__('True')):
            columns = get_table_columns(file, year, columns)
        if(config['Output']['CreatePostgresTable'].__eq__('True')):
            table_columns = get_columns(file, year, table_columns)
    
    logger.info("Dictionary of columns for survey_table created")

    #create csv files with the column names
    if(config['Output']['CreateCsv'].__eq__('True')):
        create_csv_files(csv_folderpath, columns, tables_to_merge)
        logger.info("All CSV files created")

    if(config['Output']['CreatePostgresTable'].__eq__('True')):
        create_tables(table_columns)
        logger.info("All tables created in postgres")

    for file in helper.iterate_folder(accessdb_folderpath, file_extension=".accdb"):
        logger.info("File found: " + file)
        year = file.split('\\')[-1].split('.')[0][-6:-2]
        if(config['Output']['CreateCsv'].__eq__('True')):
            extract_and_save_data(file, year, csv_folderpath, tables_to_merge, columnList = columns)
    logger.info("Data extracted and saved")  
            
if __name__ == "__main__":
    main()
