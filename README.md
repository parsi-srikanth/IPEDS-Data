# IPEDS-Data

## Introduction
This project denormalizes and combines multiple years of IPEDS databases into single source for easier reporting and analysis. 

## How to Install and Execute the Project
Below, you can find the steps to execute the program:
1. Clone the repository:
```git clone https://github.com/parsi-srikanth/IPEDS-Data.git ```
2. In this step, you create a new environment and name it ```ipeds-venv```. For this purpose, open terminal and type the below command:
   ```
   python -m venv ipeds-venv --prompt="ipeds"
   ```
3. Navigate to the venv directory created.
   ```
   cd ipeds-venv/Scripts
   ```
4. Activate the environment
   ```
   activate
   ```
5. Navigate back to IPEDS-Data directory.
6. Then, you can use requirements.txt file to install required dependencies via pip:
   ```
   python -m pip install -r requirements.txt 
   ```
8. Then, run the main.py within the directory in ipeds-venv environemnt:
   ```
   python main.py
   ```
