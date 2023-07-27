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
        df_column_names = ['time', 'log_version', 'log_code', 'log_data1', 'log_data2', 'log_data3']
        devices = ['hset', 'hphire']
        
        # Get clean file name
        file_name = os.path.splitext(os.path.basename(file_path))[0].replace('csv', '')
        
        try:
            # Extract type, id, date from file name 
            DEVICE_TYPE, DEVICE_ID, DATE = file_name.split('_')
            title = '_'.join([DEVICE_TYPE, DEVICE_ID])
            
            if DEVICE_TYPE.lower() not in devices:
                print(f'{file_name} file in data folder is for unrecognized device "{DEVICE_TYPE}"')
                return None
        
        except:
            print(f'{file_name} file in data folder is in incorrect format. Correct format: <DEVICE_TYPE>_<DEVICE_ID>_<DATE>.csv')   
            return None
                
        # Read data frame
        df = pd.read_csv(file_path, header=None, names=df_column_names)
        
        # Convert the 'time' column to datetime
        df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S', errors='coerce')

        # Add the date to the 'time' column
        df['time'] = pd.to_datetime(DATE + ' ' + df['time'].dt.strftime('%H:%M:%S'))
        
        data = {
            'DEVICE_TYPE' : DEVICE_TYPE,
            'DEVICE_ID'   : DEVICE_ID,
            'DATE'        : DATE,
            'DataFrame'   : df,
            'Title'       : title
            }
        
        return data