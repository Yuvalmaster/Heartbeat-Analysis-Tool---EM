import os
import pandas as pd
        
class LogParser:     
    # --- METHOD: load log and extract the DEVICE_TYPE, DEVICE_ID, DATE and data.
    
    @staticmethod    
    def load_log(file_path: str) -> dict:
        """
        Load log file and extract the DEVICE_TYPE, DEVICE_ID, DATE, and data.

        Parameters:
            file_path (str): The file path of the log file.

        Returns:
            dict or None: A dictionary containing extracted information or None if the file format is incorrect or the device is unrecognized.
        """
        # Set data frame columns and devices
        df_column_names = ['time_column', 'log_version', 'log_code', 'log_data1', 'log_data2', 'log_data3']
        devices = ['hset', 'hphire']
        
        # Get clean file name
        file_name = os.path.splitext(os.path.basename(file_path))[0].replace('csv', '')
        
        try:
            # Extract type, id, date from file name 
            DEVICE_TYPE, DEVICE_ID, DATE = file_name.split('_')
            title = '_'.join([DEVICE_TYPE, DEVICE_ID])
            
            if DEVICE_TYPE.lower() not in devices:
                print(f'\nLogParser -> load_log: {file_name} file in data folder is for unrecognized device "{DEVICE_TYPE}"')
                return None
        
        except:
            print(f'\nLogParser -> load_log: {file_name} file in data folder is in incorrect format. Correct format: <DEVICE_TYPE>_<DEVICE_ID>_<DATE>.csv')   
            return None
                
        # Read data frame
        df = pd.read_csv(file_path, header=None, names=df_column_names)
        
        # Convert the 'time_column' column to datetime
        df['time_column'] = pd.to_datetime(df['time_column'], format='%H:%M:%S', errors='coerce')

        # Add the date to the 'time_column' column
        df['time_column'] = pd.to_datetime(DATE + ' ' + df['time_column'].dt.strftime('%H:%M:%S'))
        
        df = df.assign(device_type=DEVICE_TYPE.lower())
        df = df.assign(device_id=DEVICE_ID)
        
        # Handle value types for uniformity
        df['log_code'] = df['log_code'].astype(str)
        df['log_version'] = df['log_version'].astype(str)
        
        data = {
            'DEVICE_TYPE' : DEVICE_TYPE.lower(),
            'DEVICE_ID'   : DEVICE_ID,
            'DATE'        : DATE,
            'DataFrame'   : df,
            'Title'       : title
            }
        
        return data