import configparser

# CREATE OBJECT
config_file = configparser.ConfigParser()

# ADD SECTION
config_file.add_section("Access DBs")

# ADD SETTINGS TO SECTION
config_file.set("Access DBs", "FolderPath", r"C:\Users\C00541311\Desktop\AccessDBs")
config_file.set("Access DBs", "PathToSaveCSV", r"C:\Users\C00541311\Desktop\AccessDBs\CSVs\<survey-name>")
# Note: You should replace <survey-name> with the actual survey name or use a placeholder if needed.

# ADD NEW SECTION AND SETTINGS
config_file.add_section("Logger")
config_file.set("Logger", "LogFilePath", "./")
config_file.set("Logger", "LogFileName", "Debug.log")
config_file.set("Logger", "LogLevel", "INFO")

# Add section for IPEDS postgres db connection via sqlalchemy
config_file.add_section("IPEDS DB")
config_file.set("IPEDS DB", "host", "localhost")
config_file.set("IPEDS DB", "port", "5432")
config_file.set("IPEDS DB", "dbname", "ipeds")
config_file.set("IPEDS DB", "user", "postgres")
config_file.set("IPEDS DB", "password", "qwerty123")

config_file.add_section("Output")
config_file.set("Output", "CreateCsv", "True")  # Note: 'True' as a string
config_file.set("Output", "CreatePostgresTable", "True")  # Note: 'True' as a string

# SAVE CONFIG FILE
with open("configurations.ini", 'w') as configfileObj:
    config_file.write(configfileObj)

print("Config file 'configurations.ini' created")
