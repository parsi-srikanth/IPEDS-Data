import csv
import os
import re
import pandas as pd
import logging

logger = logging.getLogger(__name__)

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
        logger.info(f"Created CSV file {file_name}") 

