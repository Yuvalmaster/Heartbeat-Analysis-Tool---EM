# Heartbeat Analysis Tool
## Table of Contents
- [Project Overview](#1-project-overview)
- [Getting Started](#2-getting-started)
- [How to connect to Grafana](#3-how-to-connect-to-grafana)
- [PostgreSQL Database Setup](#4-postgresql-database-setup)
- [Assumptions and Clarifications](#5-assumptions-and-clarifications)

## 1. Project Overview
The project aims to analyze and process heartbeat data collected from various devices, such as HSet and HPhire, and store it in a PostgreSQL database for further analysis. The data consists of log files in CSV format, containing device information, timestamp, heartbeat rates, and other relevant data. The main goal is to perform a comprehensive analysis and provide insights into the heart rate patterns and trends over time for different devices. Currently, the project focuses on two analyses: 1. Graph of heart rate over time. 2. graph of total heartbeats over time.

### Key Features:

* **Data Loading and Preprocessing**: The data_loader.py module loads raw data from CSV files in the `data` directory and preprocesses it to extract device information, dates, and heart rate measurements.

* **Database Interaction**: The database.py module handles interactions with the PostgreSQL database. It creates a new database if it doesn't exist, saves raw data, and stores processed analysis results.

* **Analysis and Data Processing**: The analysis.py module performs data processing tasks, including filtering and aggregating data to calculate total heartbeats and heart rate over time. It handles irregular data and gaps in measurements.

* **Configuration Management**: YAML configuration files (`db_config.yaml` and `analysis_config.yaml`) in the `config` directory allow users to specify database connection details and analysis parameters, adapting the project to different setups and requirements.

* **Visualization**: The project generates plots of hearbeat rate over time and total heartbeats per hour. The graphs are visualized using Grafana dashboard.

* **Error Handling**: The code is designed with robust error handling to catch and report any exceptions or inconsistencies in the data, ensuring smooth execution and data integrity.

### Project structure:
```md
project_root/
    |- config/
    |   |- db_config.yaml
    |   |- analysis_config.yaml
    |
    |- data/
    |   |- device1_id1_date1.csv
    |   |- device1_id1_date2.csv
    |   |- device1_id2_date1.csv
    |   |- ...
    |
    |- src/
    |   |- data_loader.py
    |   |- database.py
    |   |- analysis.py
    |   |- log_parser.py
    |   |- setup.py
    |   |- main.py
    |
    |- Dashboard data/
    |   |-.json
    |
    |- README.md
```
### Dependencies
The following Python packages are required for this project:
```md
numpy
pandas
tqdm
PyYAML
psycopg2
```
## 2. Getting Started
To use the Heartbeat Analysis Tool, follow these steps:

1. Create if missing or modify the db_config.yaml and analysis_config.yaml files from the `config` folder to configure the parameters required parameters for the project.
>> * Database Configuration: Modify the db_config.yaml file in the config directory to specify the PostgreSQL database connection details (host, port, database name, username, and password).

>> * Analysis Configuration: Adjust the analysis_config.yaml file in the config directory to set parameters such as heart rate units, heartbeat thresholds, and sample lengths for resampling.

db_config.yaml format:
```md
host: <host>
port: <port>
database: HRDataEM
user: <username>
password: <password>
```

analysis_config.yaml format (with default values):
```md
hr_param_dict:
  s: 1
  m: 60
  h: 3600
  SEC: 1
  MIN: 60
start_code:
  - '1.7.0.0'
  - '170'
end_code:
  - '1.7.1.0'
  - '171'
meas_code:
  - '1.7.0.1'
  - '200'
total_beats_code: '1.7.0.2'
cup:
  - 5
  - 6
sample_len: 10
```

2. Ensure you have Python installed (Python 3.7 or higher).

3. Data Preparation: Place the raw data CSV files in the `data` directory. Ensure that the file names follow the format `<DEVICE_TYPE>_<DEVICE_ID>_<DATE>.csv`. For demonstration purposes, I added several examples in the provided `data` directory.

4. Running the Project: Execute the `ran.bat` file to load the data, perform analysis, and store the results in the specified PostgreSQL database. The main.py will install the missing packages that are required for the project.

5. Visualization: Connect to Grafana Dashboard, and visualize the data.

## 3. How to connect to Grafana
1. Download grafana https://grafana.com/grafana/download/10.0.0?pg=oss-graf&plcmt=hero-btn-1&platform=windows
2. select a Grafana Version you want to install (the most recent Grafana version is selected by default).
3. Open your web browser and go to http://localhost:3000/. 3000 is the default HTTP port that Grafana listens to if you haven’t configured a different port.
4. On the login page, type admin for the username and password. Change your password later.
5. Open the side menu on the left and open dashboards.
6. In the Dashboards click on New -> Import.
7. Upload the JSON file from the `Dashboard data` folder.
8. Set a name for the dashboard, and connect to PostgreSQL.
9. Explore the dashboard by using the filters located at the top. You have the flexibility to choose Device, ID, multiple Date, Version, Recording_id options, and units for the heartbeat rate graph. Additionally, you can zoom in and out on the Heartbeat Rate Over Time graph based on your preferences for recording_id or time.
    
![Dashboard_img](https://github.com/Yuvalmaster/Heartbeat-Analysis-Tool---EM/assets/121662835/c9786a22-838e-429d-8bda-0fc263f30a1e)

## 4. PostgreSQL Database Setup:
1. To set up the PostgreSQL database for the Heartbeat Analysis Tool, follow these steps:
2. Install PostgreSQL: If you haven't already installed PostgreSQL, you can download it from the official website: https://www.postgresql.org/download/
3. Create a Database: Open a terminal or command prompt and use the following commands to create a new database for the project:

```md
# Replace <database_name>, <username>, and <password> with your desired values.
psql -U postgres -c "CREATE DATABASE <database_name>;"
psql -U postgres -c "CREATE USER <username> WITH PASSWORD '<password>';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE <database_name> TO <username>;"
```

## 5. Assumptions and Clarifications
1. CSV files hold measurements for specific dates. The files may include several tests on the same day and may overlap with tests from the day before or after.

2. Both devices record heartbeats every 10 seconds during varying test durations (from seconds to days).

3. The script handles irregular data without assuming a fixed log entry interval.

4. If multiple measurements occur simultaneously, the script selects the first value for simplicity. Future versions may require considering average or median options.

5. Heartbeat rates exceeding device-specific limits get capped to those limits.

6. Total beats per hour rely on HSet's total beats measurement, or rate * time for Hphire devices or in case of gaps between regular measurements.

7. For gaps exceeding 20 seconds between measurements, the script adds new points with zero beats.

8. Optional log data 3 is ignored for analysis if empty.

9. Duplicate data from the same device on the same date is rejected to avoid repetition.

10. A user-provided db_config file ensures security and avoids password sharing.

11. The graph of the total heartbeat over time will calculate how many beats were measured during each hour of the day. If there is a partial recording, it will still show how many were recorded in that hour and indicate that it is partial. If a recording lasted an hour (from hh:00 to hh:59), it will be labeled as 'Fully recorded,' otherwise, it will be labeled as 'Partially recorded'.

12. The Heartbeat Rate Over Time graph is based on universal time (UTC).
