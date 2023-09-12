import configparser

# CREATE OBJECT
config_file = configparser.ConfigParser()

# ADD SECTION
config_file.add_section("Access DB File Path")
# ADD SETTINGS TO SECTION
config_file.set("Access DB File Path", "IPEDS202122", r"C:\Users\C00541311\Desktop\IPEDS202122\IPEDS202122.accdb")

# ADD NEW SECTION AND SETTINGS
config_file["Logger"]={
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
