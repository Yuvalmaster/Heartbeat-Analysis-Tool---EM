import os
import numpy as np
import pandas as pd
import yaml
from datetime import date, time, datetime


class Analysis:
    def __init__(self, db_connection, table_name: str, config_directory: str):
        """
        Initialize the Analysis class.

        Parameters:
            db_connection (connection): The database connection object.
            table_name (str): The name of the table in the database.
            config_directory (str): The directory containing the analysis configuration YAML file.
        """
        if db_connection is None:
            raise ValueError("Invalid database connection. Please provide a valid database connection.")
        self.db_connection = db_connection

        if not table_name:
            raise ValueError("Invalid table name. Please provide a non-empty table name.")
        self.table_name = table_name

        # Load constants from the config file, providing default values if values are missing in the config file.
        try:
            with open(os.path.join(config_directory, 'analysis_config.yaml'), "r") as yaml_file:
                config = yaml.safe_load(yaml_file)
        except FileNotFoundError:
            raise FileNotFoundError("The analysis configuration YAML file could not be found. Please make sure the file exists in the specified directory.")

        if 'hr_param_dict' in config:
            self.hr_param_dict = config['hr_param_dict']
        else:
            self.hr_param_dict = {
                's'  : 1,
                'm'  : 60,
                'h'  : 3600,
                'SEC': 1,
                'MIN': 60
            }  
        
        if 'start_code' in config:
            self.start_code = config['start_code']
        else:
            self.start_code = ['1.7.0.0', '170']

        if 'end_code' in config:
            self.end_code = config['end_code']
        else:
            self.end_code = ['1.7.1.0', '171']

        if 'meas_code' in config:
            self.meas_code = config['meas_code']
        else:
            self.meas_code = ['1.7.0.1', '200']

        if 'total_beats_code' in config:
            self.total_beats_code = config['total_beats_code']
        else:
            self.total_beats_code = '1.7.0.2'

        # Threshold for beats per second per device
        if 'cup' in config:
            self.cup = config['cup']
        else:
            self.cup = [5, 6]

        # sampling rate in seconds
        if 'sample_len' in config:
            self.sample_len = config['sample_len']
        else:
            self.sample_len = 10

        self.filtered_df = self.filter_data()


    def filter_data(self) -> pd.DataFrame:
        """
        Filter and process the raw data from the table.

        Returns:
            DataFrame: Filtered DataFrame with processed data.
        """
        query = f"""
            SELECT *
            FROM {self.table_name};
        """
        try:
            # Get data from database
            df = pd.read_sql_query(query, self.db_connection)
            
            # Handle value types for uniformity
            df['log_code'] = df['log_code'].astype(str)
            df['log_version'] = df['log_version'].astype(str)

        except Exception as error:
            raise Exception(f"Error while fetching data from the database: {str(error)}")
        
        name = self.table_name.split('_')
        df = df.assign(device_type=name[0].upper())
        df = df.assign(device_id=name[1])

        df['time_diff[sec]'] = pd.to_datetime(df['time_column'], format='%H:%M:%S').diff().dt.total_seconds()

        # Initiate variables
        count = 0
        hr_list = []
        recording_id = []
        total_beats_device = []
        valid_flag = False
        
        # Filter invalid rows and assign recording_id labels
        for idx, code in enumerate(df['log_code']):
            # If found a start code, create a new label
            if code in self.start_code:
                valid_flag = True
                count += 1
                recording_id.append(count)
                hr_list.append(np.nan)
                total_beats_device.append(np.nan)

            # If code is a measurement code, and the valid_flag is on, it saves the heart rate in beats/sec and deals with irregular data.
            elif code in self.meas_code and valid_flag:
                # remove entries at the same time
                if df['time_diff[sec]'][idx] == 0 and df['log_code'][idx-1] not in self.start_code:
                    recording_id.append('remove')
                    hr_list.append(np.nan)
                    total_beats_device.append(np.nan)
                else:
                    hr = df['log_data1'][idx] / self.hr_param_dict[df['log_data2'][idx]]  # Heart rate, normalized to beats/sec
                    recording_id.append(count)
                    total_beats_device.append(np.nan)

                    # Check cup for Hset devices
                    if code == self.meas_code[0] and hr > self.cup[0]:
                        hr_list.append(self.cup[0])

                    # Check cup for HPhire devices
                    elif code == self.meas_code[1] and hr > self.cup[1]:
                        hr_list.append(self.cup[0])

                    else:
                        hr_list.append(round(hr, 3))

                        # =============================================== #
                        # NOTE: lowest beats per second ever measured for
                        # a living person was 0.45 [beats/sec] (27
                        # [beats/sec]). Therefore, for future revisions
                        # it might be relevant to consider a low threshold.
                        # =============================================== #

            elif code in self.total_beats_code and valid_flag:
                total_beats_device.append(df['log_data1'][idx])
                hr_list.append(np.nan)
                recording_id.append(count)

            # If code is end code, it turns off the valid_flag, meaning no measurement will register until the flag is on.
            elif code in self.end_code:
                valid_flag = False
                recording_id.append(count)
                hr_list.append(np.nan)
                total_beats_device.append(np.nan)

            # Remove items that have no valid_flag
            elif not valid_flag:
                recording_id.append('remove')
                hr_list.append(np.nan)
                total_beats_device.append(np.nan)

        df['beats_sec'] = hr_list
        df = self.convert_units(df, 'm')
        df = self.convert_units(df, 'h')
        df['total_beats_device'] = total_beats_device
        df['recording_id'] = recording_id

        filtered_df = df[~df['recording_id'].isin(['remove'])]

        # Address time diff greater than 20 seconds
        filtered_df.loc[(filtered_df['time_diff[sec]'] > 20) & (filtered_df['log_code'].isin(self.meas_code)), 'time_diff_[sec]'] = 20

        # Calculate total number of beats for each measurement
        mask = filtered_df['time_diff[sec]'].notna() & filtered_df['beats_sec'].notna()
        with pd.option_context('mode.chained_assignment', None):
            filtered_df['total_beats'] = np.floor(filtered_df['time_diff[sec]'][mask] * filtered_df['beats_sec'][mask])

        # Iterate through each column in the DataFrame and remove irrelevant columns
        for col in filtered_df.columns:
            if filtered_df[col].isna().all():
                filtered_df.drop(columns=col, inplace=True)

        filtered_df.reset_index(inplace=True, drop=True)
        return filtered_df


    def calc_total_heartbeat_over_time(self) -> pd.DataFrame:
        """
        Calculate the total heartbeat over time for each recording.

        Returns:
            DataFrame: DataFrame with total heartbeat over time for each recording.
        """
        total_tests = self.filtered_df['recording_id'].unique()
        total_heart_beat_df = []

        for test in total_tests:
            test_df = self.filtered_df[self.filtered_df['recording_id'] == test]
            # Group the data by date and hour
            hourly_groups = test_df.groupby([test_df['time_column'].dt.date, test_df['time_column'].dt.hour])

            for (date_val, hour), hour_group in hourly_groups:
                # The subsequent section deals with HSet devices and the total beats code (1.7.0.2).
                # If code 1.7.0.2 is not the last code before ending the test, the script will finalize the total beats.
                if 'total_beats_device' in hour_group.columns:
                    last_value_index = hour_group['total_beats_device'].last_valid_index()

                    if not last_value_index:
                        total_beats = hour_group['total_beats'].sum()
                    
                    elif last_value_index != hour_group.index[-1] - 1:
                        total_beats = hour_group['total_beats_device'].sum() + hour_group.loc[last_value_index:, 'total_beats'].sum()
                    
                    else:
                        total_beats = hour_group['total_beats_device'].sum()
                
                else:
                    total_beats = hour_group['total_beats'].sum()

                total_time = hour_group['time_column'].max() - hour_group['time_column'].min()
                start_datetime = hour_group['time_column'].min()
                end_datetime = hour_group['time_column'].max()

                version = hour_group['log_version'].unique().item()
                device = hour_group['device_type'].unique().item()
                device_id = hour_group['device_id'].unique().item()

                # flag that indicates if a complete hour was measured.
                is_hour_complete = hour_group['time_column'].dt.minute.min() == 0 and hour_group['time_column'].dt.minute.max() == 59

                total_heart_beat_df.append({
                    'date_column'     : date_val.strftime('%Y-%m-%d'),
                    'recording_id'    : test,
                    'hour_column'     : hour,
                    'total_beats'     : total_beats,
                    'total_time'      : total_time,
                    'start_time'      : start_datetime,
                    'end_time'        : end_datetime,
                    'device_type'     : device,
                    'device_id'       : device_id,
                    'log_version'     : version,
                    'is_hour_complete': is_hour_complete
                })

        total_heart_beat_df = pd.DataFrame(total_heart_beat_df)

        return total_heart_beat_df


    def calc_heartbeat_rate_over_time(self) -> pd.DataFrame:
        """
        Calculate the heartbeat rate over time for each recording.

        Returns:
            DataFrame: DataFrame with heartbeat rate over time for each recording.
        """
        df = self.filtered_df[['time_column', 'log_version', 'log_code', 'beats_sec', 'beats_min','beats_hr', 'recording_id', 'device_type', 'device_id']]

        start_code_indexes = df['log_code'].isin(self.start_code)
        # Add the first measured point to the starting time. 
        df.loc[start_code_indexes, 'beats_sec'] = df['beats_sec'].shift(-1).where(df['log_code'].shift(-1).isin(self.meas_code))
        df.loc[start_code_indexes, 'beats_min'] = df['beats_min'].shift(-1).where(df['log_code'].shift(-1).isin(self.meas_code))
        df.loc[start_code_indexes, 'beats_hr'] = df['beats_hr'].shift(-1).where(df['log_code'].shift(-1).isin(self.meas_code))
        
        # Find gaps with over 20 seconds and add zero for all measurments after this point.
        gap_mask = (df['time_column'].diff().dt.total_seconds() > 20) & (~df['log_code'].isin(self.start_code))
        gap_rows = []

        for idx in df.index[gap_mask]:
            prev_time = df['time_column'].iloc[idx - 1]
            new_time = prev_time + pd.Timedelta(seconds=20)

            new_row = {
                'time_column' : new_time,
                'log_code'    : 'added_point',
                'beats_sec'   : 0,
                'beats_min'   : 0,
                'beats_hr'    : 0,
                'recording_id': df['recording_id'][idx],
                'log_version' : df['log_version'][idx],
                'device_type' : df['device_type'][idx],
                'device_id'   : df['device_id'][idx],
            }

            gap_rows.append(new_row)

        gap_rows_df = pd.DataFrame(gap_rows)
        heartbeat_rate_df = pd.concat([df, gap_rows_df], ignore_index=True)

        heartbeat_rate_df.sort_values(by='time_column', inplace=True)
        heartbeat_rate_df = self.resample_df(heartbeat_rate_df, self.sample_len, (self.start_code, self.end_code))

        return heartbeat_rate_df


    @staticmethod
    def resample_df(df: pd.DataFrame, sec: int, codes: tuple) -> pd.DataFrame:
        """
        Resample the DataFrame by adding rows with measurements in every 'sec' seconds.

        Parameters:
            df (DataFrame): The DataFrame to resample.
            sec (int): The time interval in seconds for resampling.
            codes (tuple): A tuple containing start and end codes.

        Returns:
            DataFrame: The resampled DataFrame.
        """
        df['time_diff[sec]'] = df['time_column'].diff().dt.total_seconds()
        df.reset_index(inplace=True, drop=True)
        gap_rows = []
        
        lookout_idx = df.index[(df['time_diff[sec]'] > sec) & (~df['log_code'].isin(codes[0])) & (~df['log_code'].isin(codes[1]))]

        for idx in lookout_idx:
            prev_time = df['time_column'].iloc[idx - 1]
            num_rows = int(df['time_diff[sec]'].iloc[idx] // sec) - 1
            time_interval = pd.Timedelta(seconds=sec)

            for i in range(num_rows):
                new_time = prev_time + (i + 1) * time_interval
                recording_id = df['recording_id'].iloc[idx]

                new_row = {
                    'time_column' : new_time,
                    'log_version' : df['log_version'][idx],
                    'log_code'    : 'added_point', 
                    'beats_sec'   : df['beats_sec'][idx-1],
                    'beats_min'   : df['beats_min'][idx-1],
                    'beats_hr'    : df['beats_hr'][idx-1],
                    'recording_id': recording_id,
                    'device_type' : df['device_type'][idx],
                    'device_id'   : df['device_id'][idx],
                }

                gap_rows.append(new_row)

        gap_rows_df = pd.DataFrame(gap_rows)
        sampled_df = pd.concat([df, gap_rows_df], ignore_index=True)

        sampled_df.sort_values(by='time_column', inplace=True)
        sampled_df.reset_index(inplace=True, drop=True)
        sampled_df.drop('time_diff[sec]', axis=1, inplace=True)

        return sampled_df

    @staticmethod
    def convert_units(df: pd.DataFrame, unit: str) -> pd.DataFrame: 
        """
        Convert heartbeat rate (HR) values in the DataFrame from one time unit to another.

        Parameters:
            df (DataFrame): The DataFrame containing heartbeat rate data.
            unit (str): The target time unit to which the HR values need to be converted.

        Returns:
            DataFrame: The DataFrame with converted HR values.
        """
        valid_units = ['m', 'h']
        if unit not in valid_units:
            raise ValueError(f"Invalid time unit. Expected {valid_units}, but received '{unit}'.")
            
        if unit == 'm':
            df['beats_min'] = np.floor(df['beats_sec'] * 60)
        
        elif unit == 'h':
            df['beats_hr'] = np.floor(df['beats_sec'] * 3600)
            
        return df
