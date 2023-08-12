import os
from tqdm import tqdm
import yaml

from log_parser import LogParser
from database import Database
from analysis import Analysis

class DataLoader:
    def __init__(self, data_directory: str, config_directory: str):
        """
        Initialize the DataLoader class.

        Parameters:
            data_directory (str): The directory path containing the CSV files to be loaded.
            config_directory (str): The directory path containing the database and analysis configuration files.
        """
        # Save directories paths
        self.data_directory = data_directory
        self.config_directory = config_directory

        # Load database
        self.db_config = self.load_db_config_file(self.config_directory)
        self.db = Database(self.db_config)
        
        self.last_primary_id = self.check_last_primary_id()

        # Load raw data
        self.load_all_csv()


    def is_directory_empty(self) -> bool:
        """
        Check if the data directory is empty.

        Returns:
            bool: True if the directory is empty, False otherwise.
        """
        # Check if the directory is empty by listing its contents
        files_in_directory = os.listdir(self.data_directory)
        return not any(f.lower().endswith('.csv') for f in files_in_directory)


    def load_all_csv(self):
        """
        Load all CSV files in the data folder and save the raw data directly to the database.

        Raises:
            FileNotFoundError: If no CSV files are found in the specified directory.
        """
        # Test if the directory is empty
        if self.is_directory_empty():
            raise FileNotFoundError("DataLoader -> load_all_csv: No CSV files found in the specified directory.")

        # Iterate over each CSV file in the directory
        for file_path in tqdm(os.scandir(self.data_directory), desc='Loading CSV files', unit='file'):
            try:
                if file_path.path.lower().endswith('.csv'):
                    raw_data = LogParser.load_log(file_path)
                    if raw_data:
                        self.db.save_raw_data(raw_data)
                        self.db.save_device(raw_data['DEVICE_TYPE'], raw_data['DEVICE_ID'])

            except FileNotFoundError:
                raise FileNotFoundError(f"DataLoader -> load_all_csv: File '{self.data_directory}' not found.")
            

    @staticmethod
    def load_db_config_file(config_directory: str) -> dict:
        """
        Load the database configuration file values.

        Parameters:
            config_directory (str): The directory path containing the database configuration YAML file.

        Returns:
            dict: A dictionary containing the database configuration values.
        """
        # Load config values
        with open(os.path.join(config_directory, 'db_config.yaml'), "r") as yaml_file:
            config_data = yaml.safe_load(yaml_file)

            config = {
                'host'       : config_data.get("host"),
                'port'       : config_data.get("port"),
                'database'   : config_data.get("database"),
                'user'       : config_data.get("user"),
                'password'   : config_data.get("password"),
                'schema'     : config_data.get("schema"),
                'table_names': config_data.get("table_names")
            }

            return config


    def run_analysis_for_all_devices(self):
        """
        Request all tables from the database and initiate analysis for each device.

        Returns:
            None
        """
        # Get all devices in the database
        devices_list = self.db.find_all_devices()

        # Handle missing devices list
        if not devices_list:
            print('DataLoader -> run_analysis_for_all_devices: Error in loading devices from the database')
            return None
           
        for device in devices_list:           
            analysis_obj = Analysis(db_config=self.db_config,
                                    device=device, 
                                    config_directory=self.config_directory,
                                    last_primary_id=self.last_primary_id)
            
            
            if analysis_obj.filtered_df.empty:
                continue
            
            analysis_obj.calc_total_heartbeat_over_time()
            analysis_obj.calc_heartbeat_rate_over_time()                      
            analysis_obj.close_db()
            
            
    def check_last_primary_id(self):
        """
        Check the last primary ID for the specified end codes in the log data.

        Returns:
            int or None: Last primary ID if the table exists, None otherwise.
        """
        with open(os.path.join(self.config_directory, 'analysis_config.yaml'), "r") as yaml_file:
            config = yaml.safe_load(yaml_file)
            
        if 'end_code' in config:
            end_code = config['end_code']
        else:
            end_code = ['1.7.1.0', '171']
            
        table_exists = self.db.table_exists(self.db.schema_name, self.db.table_names[2])

        if table_exists:
            # Execute the query to get the last ID
            end_codes = "', '".join(end_code)
            
            query_last_id = f"""
                SELECT id_primary
                FROM {self.db.schema_name}.{self.db.table_names[1]}
                WHERE log_code IN ('{end_codes}')
                ORDER BY id_primary DESC
                LIMIT 1;
            """
            self.db.cursor.execute(query_last_id)
            last_id = self.db.cursor.fetchone()[0]  # Fetch the first column of the first row
            return last_id
        
        else:
            print(f"DataLoader -> check_last_primary_id: {self.db.table_names[1]} table does not exist.")
            return None            
