import configparser

# CREATE OBJECT
config_file = configparser.ConfigParser()

# ADD SECTION
config_file.add_section("Access DBs")
# ADD SETTINGS TO SECTION
config_file.set("Access DBs", "#", "Path to Access DBs Folder")
config_file.set("Access DBs", "FolderPath", r"C:\Users\C00541311\Desktop\AccessDBs")
config_file.set("Access DBs", "PathToSaveCSV", r"C:\Users\C00541311\Desktop\AccessDBs\CSVs\survey")
# ADD NEW SECTION AND SETTINGS
config_file["Logger"]={
    "#":"Path to the log file and log level information",
        "LogFilePath":"./",
        "LogFileName" : "Debug.log",
        "LogLevel" : "INFO"
        }

# SAVE CONFIG FILE
with open(r"configurations.ini", 'w') as configfileObj:
    config_file.write(configfileObj)
    configfileObj.flush()
    configfileObj.close()

print("Config file 'configurations.ini' created")
