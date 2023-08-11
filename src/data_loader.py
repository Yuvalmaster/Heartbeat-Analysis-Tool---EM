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
        self.db_config = self.load_config_file(self.config_directory)
        self.db = Database(self.db_config)

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
            raise FileNotFoundError("No CSV files found in the specified directory.")

        # Iterate over each CSV file in the directory
        for file_path in tqdm(os.scandir(self.data_directory), desc='Loading CSV files', unit='file'):
            try:
                if file_path.path.lower().endswith('.csv'):
                    raw_data = LogParser.load_log(file_path)
                    if raw_data:
                        self.db.save_raw_data(raw_data)
                        self.db.save_device(raw_data['DEVICE_TYPE'], raw_data['DEVICE_ID'])

            except FileNotFoundError:
                raise FileNotFoundError(f"File '{self.data_directory}' not found.")
            

    @staticmethod
    def load_config_file(config_directory: str) -> dict:
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
                'host'    : config_data.get("host"),
                'port'    : config_data.get("port"),
                'database': config_data.get("database"),
                'user'    : config_data.get("user"),
                'password': config_data.get("password")
            }

            return config


    @staticmethod
    def run_analysis_for_all_devices(db: Database, config_directory: str):
        """
        Request all tables from the database and initiate analysis for each device.

        Parameters:
            db (Database): The Database object used for interacting with the database.
            config_directory (str): The directory path containing the analysis configuration YAML file.

        Returns:
            None
        """
        # Get all devices in the database
        devices_list = db.find_all_devices()

        # Handle missing devices list
        if not devices_list:
            print('Error in loading devices from the database')
            return None
           
        for device in devices_list:
            analysis_obj = Analysis(db.connection, device, config_directory)
            
            total_heart_beat_df = analysis_obj.calc_total_heartbeat_over_time()
            heartbeat_rate_df = analysis_obj.calc_heartbeat_rate_over_time()
            
            db.save_analysis_data(total_heart_beat_df, device, 'total')
            db.save_analysis_data(heartbeat_rate_df, device, 'rate')
            
             
