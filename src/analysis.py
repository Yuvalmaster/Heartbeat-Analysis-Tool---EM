import os
import numpy as np
import pandas as pd
import yaml

from database import Database

class Analysis(Database):
    def __init__(self, db_config: dict, device: tuple, config_directory: str, last_primary_id: int):
        super().__init__(db_config)
        """
        Initialize the Analysis class.

        Parameters:
            db_config (dict): Configuration parameters for the database connection.
            device (tuple): A tuple containing the device type and device ID.
            config_directory (str): The directory containing the analysis configuration YAML file.
            last_primary_id (int): The last primary ID used for limiting the analysis to new data only.
        """
        self.DEFAULT_CONFIG = {
            'hr_param_dict': {
                's': 1,
                'm': 60,
                'h': 3600,
                'SEC': 1,
                'MIN': 60
            },
            'start_code': ['1.7.0.0', '170'],
            'end_code': ['1.7.1.0', '171'],
            'meas_code': ['1.7.0.1', '200'],
            'total_beats_code': '1.7.0.2',
            'cap': [5, 6],
            'sample_rate': 10,
            'time_delta': 20.0
            }
              
        if not device:
            raise ValueError("Analysis -> __init__: Please provide a tuple of (<device_type>, <device_id>).")
        self.device = device

        # Load constants from the config file, providing default values if values are missing in the config file.
        try:
            with open(os.path.join(config_directory, 'analysis_config.yaml'), "r") as yaml_file:
                config = yaml.safe_load(yaml_file)
        except FileNotFoundError:
            raise FileNotFoundError("Analysis -> __init__: The analysis configuration YAML file could not be found. Please make sure the file exists in the specified directory.") 
        
        # Fetch values from config. If missing get defaults.
        self.hr_param_dict = config.get('hr_param_dict', self.DEFAULT_CONFIG['hr_param_dict'])
        self.start_code = config.get('start_code', self.DEFAULT_CONFIG['start_code'])
        self.end_code = config.get('end_code', self.DEFAULT_CONFIG['end_code'])
        self.meas_code = config.get('meas_code', self.DEFAULT_CONFIG['meas_code'])
        self.total_beats_code = config.get('total_beats_code', self.DEFAULT_CONFIG['total_beats_code'])
        self.cap = config.get('cap', self.DEFAULT_CONFIG['cap'])
        self.sample_rate = config.get('sample_rate', self.DEFAULT_CONFIG['sample_rate'])
        self.delta = float(config.get('time_delta', self.DEFAULT_CONFIG['time_delta']))


        self.last_primary_id = last_primary_id
        self.filtered_df = self.filter_data()
 
           
    # %% ANALYSIS METHODS
    
    def filter_data(self) -> pd.DataFrame:
        """
        Filter and process the raw data from the table.

        Returns:
            DataFrame: Filtered DataFrame with processed data.
        """
        
        df = self.load_data_for_analysis(self.device, self.last_primary_id)
        if df.empty:
            filter_data = df
            return filter_data
      
        df['time_diff[sec]'] = pd.to_datetime(df['time_column'], format='%H:%M:%S').diff().dt.total_seconds()

        # Initiate variables
        test_count = self.check_last_test_id(self.device)
        hr_list = []
        test_id = []
        total_beats_device = []
        valid_flag = False
        
        # Filter invalid rows and assign test_id labels
        for idx, code in enumerate(df['log_code']):
            # If found a start code, create a new label
            if code in self.start_code:
                valid_flag = True
                test_count += 1
                test_id.append(test_count)
                hr_list.append(np.nan)
                total_beats_device.append(np.nan)

            # If code is a measurement code, and the valid_flag is on, it saves the heart rate in beats/sec and deals with irregular data.
            elif code in self.meas_code and valid_flag:
                # remove entries at the same time
                if df['time_diff[sec]'][idx] == 0 and df['log_code'][idx-1] not in self.start_code:
                    test_id.append('remove')
                    hr_list.append(np.nan)
                    total_beats_device.append(np.nan)
                else:
                    hr = df['log_data1'][idx] / self.hr_param_dict[df['log_data2'][idx]]  # Heart rate, normalized to beats/sec
                    test_id.append(test_count)
                    total_beats_device.append(np.nan)

                    # Check cap for Hset devices
                    if code == self.meas_code[0] and hr > self.cap[0]:
                        hr_list.append(self.cap[0])

                    # Check cap for HPhire devices
                    elif code == self.meas_code[1] and hr > self.cap[1]:
                        hr_list.append(self.cap[0])

                    else:
                        hr_list.append(round(hr, 3))

                        # =============================================== #
                        # NOTE: lowest beats per second ever measured for
                        # a living person was 0.45 [beats/sec] (27
                        # [beats/min]). Therefore, for future revisions
                        # it might be relevant to consider a low threshold.
                        # =============================================== #

            elif code in self.total_beats_code and valid_flag:
                total_beats_device.append(df['log_data1'][idx])
                hr_list.append(np.nan)
                test_id.append(test_count)

            # If code is end code, it turns off the valid_flag, meaning no measurement will register until the flag is on.
            elif code in self.end_code:
                valid_flag = False
                test_id.append(test_count)
                hr_list.append(np.nan)
                total_beats_device.append(np.nan)

            # Remove items that have no valid_flag
            elif not valid_flag:
                test_id.append('remove')
                hr_list.append(np.nan)
                total_beats_device.append(np.nan)

        df['beats_sec'] = hr_list
        df = self.convert_units(df, 'm', valid_units=self.hr_param_dict)
        df = self.convert_units(df, 'h', valid_units=self.hr_param_dict)
        df['total_beats_device'] = total_beats_device
        df['test_id'] = test_id

        filtered_df = df[~df['test_id'].isin(['remove'])]
        filtered_df.reset_index(inplace=True, drop=True)
        
        # Check ongoing test and apply flag
        filtered_df = filtered_df.assign(ongoing=False)
        ongoing_loc = self.check_ongoing_test(filtered_df)
        if ongoing_loc != None:
            filtered_df.loc[ongoing_loc:, 'ongoing'] = True
            

        filtered_df = self.handle_data_gaps(df=filtered_df, start_code=self.start_code, delta=self.delta)

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


    def calc_total_heartbeat_over_time(self):
        """
        Calculate the total heartbeat over time for each recording and save the data to database.

        Returns:
            None
        """
        total_tests = self.filtered_df['test_id'].unique()
        total_heart_beat_df = []
        
        for test in total_tests:
            test_df = self.filtered_df[self.filtered_df['test_id'] == test]
            
            # Group the data by date and hour
            hourly_groups = test_df.groupby([test_df['time_column'].dt.date, test_df['time_column'].dt.hour])

            last_hour_device_total = 0

            for (date_val, hour), hour_group in hourly_groups:
                # The subsequent section deals with HSet devices and the total beats code (1.7.0.2).
                # If code 1.7.0.2 is not the last code before ending the test, the script will finalize the total beats.
                if 'total_beats_device' in hour_group.columns:
                    last_value_index = hour_group['total_beats_device'].last_valid_index()

                    if not last_value_index:
                        total_beats = hour_group['total_beats'].sum()
                    
                    elif last_value_index != hour_group.index[-1] - 1:
                        total_beats = (hour_group['total_beats_device'][last_value_index]-last_hour_device_total) + hour_group.loc[last_value_index:, 'total_beats'].sum()
                        last_hour_device_total = hour_group['total_beats_device'][last_value_index]
                    
                    else:
                        total_beats = (hour_group['total_beats_device'][last_value_index]-last_hour_device_total)
                        last_hour_device_total = hour_group['total_beats_device'][last_value_index]
                
                else:
                    total_beats = hour_group['total_beats'].sum()

                total_time = hour_group['time_column'].max() - hour_group['time_column'].min()
                start_datetime = hour_group['time_column'].min()
                end_datetime = hour_group['time_column'].max()
                
                version = hour_group['log_version'].unique().item() # Assuming each test is at specific version. Versions can be updated after the test ends.
                device  = hour_group['device_type'].unique().item()
                device_id = hour_group['device_id'].unique().item()

                # flag that indicates if a complete hour was measured.
                is_hour_complete = hour_group['time_column'].dt.minute.min() == 0 and hour_group['time_column'].dt.minute.max() == 59

                if hour_group['ongoing'].any():
                    ongoing = True
                else:
                    ongoing = False
                
                total_heart_beat_df.append({
                    'time_column'     : date_val.strftime('%Y-%m-%d'),
                    'test_id'         : test,
                    'hour_column'     : hour,
                    'total_beats'     : total_beats,
                    'total_time'      : total_time,
                    'start_time'      : start_datetime,
                    'end_time'        : end_datetime,
                    'device_type'     : device,
                    'device_id'       : device_id,
                    'log_version'     : version,
                    'is_hour_complete': is_hour_complete,
                    'ongoing'         : ongoing
                })

        total_heart_beat_df = pd.DataFrame(total_heart_beat_df)
        self.save_analysis_data(total_heart_beat_df, self.schema_name, self.table_names[2])


    def calc_heartbeat_rate_over_time(self) -> pd.DataFrame:
        """
        Calculate the heartbeat rate over time for each recording and save the data to database.

        Returns:
            None
        """
        df = self.filtered_df[['time_column', 'log_version', 'log_code', 'beats_sec', 'beats_min','beats_hr', 'test_id', 'device_type', 'device_id', 'time_diff[sec]', 'ongoing']]

        start_code_indexes = df['log_code'].isin(self.start_code)

        # Add the first measured point to the starting time. 
        units = ['beats_sec', 'beats_min', 'beats_hr']
        for unit in units:
            df.loc[start_code_indexes, unit] = df[unit].shift(-1).where(df['log_code'].shift(-1).isin(self.meas_code))
     
        heartbeat_rate_df = self.resample_df(df, self.sample_rate, (self.start_code, self.end_code))
        self.save_analysis_data(heartbeat_rate_df,self.schema_name, self.table_names[3])


    # %% UTILITIES

    @staticmethod
    def resample_df(df: pd.DataFrame, sample_rate: int, codes: tuple) -> pd.DataFrame:
        """
        Resample the DataFrame by adding rows with measurements in every 'sample_rate' seconds.

        Parameters:
            df (DataFrame): The DataFrame to resample.
            sample_rate (int): The time sample_rate in seconds for resampling.
            codes (tuple): A tuple containing start and end codes.

        Returns:
            DataFrame: The resampled DataFrame.
        """
        df = df.copy()
        if 'time_diff[sec]' not in df.columns:
            time_diff = Analysis.recalculate_time_diff(df)
            df['time_diff[sec]'] = time_diff
            
        gap_rows = []
        
        lookout_idx = df.index[(df['time_diff[sec]'] > sample_rate) & (~df['log_code'].isin(codes[0])) & (~df['log_code'].isin(codes[1]))]

        for idx in lookout_idx:
            prev_time = df['time_column'].iloc[idx]
            num_rows = int(df['time_diff[sec]'].iloc[idx] // sample_rate) - 1
            time_interval = pd.Timedelta(seconds=sample_rate)

            for i in range(num_rows):
                new_time = prev_time + (i + 1) * time_interval
                test_id = df['test_id'].iloc[idx]

                new_row = {
                    'time_column' : new_time,
                    'log_version' : df['log_version'][idx],
                    'log_code'    : 'added_point', 
                    'beats_sec'   : df['beats_sec'][idx],
                    'beats_min'   : df['beats_min'][idx],
                    'beats_hr'    : df['beats_hr'][idx],
                    'test_id'     : test_id,
                    'device_type' : df['device_type'][idx],
                    'device_id'   : df['device_id'][idx],
                    'ongoing'     : df['ongoing'][idx]
                }

                gap_rows.append(new_row)

        gap_rows_df = pd.DataFrame(gap_rows)
        sampled_df = pd.concat([df, gap_rows_df], ignore_index=True)

        sampled_df.sort_values(by='time_column', inplace=True)
        sampled_df.reset_index(inplace=True, drop=True)
        sampled_df.drop('time_diff[sec]', axis=1, inplace=True)

        return sampled_df


    @staticmethod
    def convert_units(df: pd.DataFrame, unit: str, valid_units:dict) -> pd.DataFrame: 
        """
        Convert heartbeat rate (HR) values in the DataFrame from one time unit to another.

        Parameters:
            df (DataFrame): The DataFrame containing heartbeat rate data.
            unit (str): The target time unit to which the HR values need to be converted.
            valid_units (dict): Valid units in DataFrames

        Returns:
            DataFrame: The DataFrame with converted HR values.
        """
        if unit not in valid_units.keys():
            raise ValueError(f"Analysis -> convert_units: Invalid time unit. Expected {valid_units.keys}, but received '{unit}'.")
            
        if unit == 'm' or unit == 'MIN':
            df['beats_min'] = np.round(df['beats_sec'] * valid_units['m'])
        
        elif unit == 'h':
            df['beats_hr'] = np.round(df['beats_sec'] * valid_units['h'])
            
        return df


    @staticmethod
    def handle_data_gaps(df: pd.DataFrame, start_code: list, delta: float) -> pd.DataFrame:
        """
        Find gaps with over delta seconds and add zero beats/sec for all measurments after each gap point.

        Parameters:
            df (DataFrame): The DataFrame containing filtered data.
            start_code (list): List of starting codes.
            delta (float): Time delta in seconds.

        Returns:
            DataFrame: The DataFrame with added points.
        """

        gap_mask = (df['time_diff[sec]'] > delta) & (~df['log_code'].isin(start_code))
        gap_rows = []

        for idx in df.index[gap_mask]:
            prev_time = df['time_column'].iloc[idx - 1]
            new_time = prev_time + pd.Timedelta(seconds=delta)

            new_row = {
                'time_column'       : new_time,
                'log_version'       : df['log_version'][idx],
                'log_code'          : 'added_point',
                'device_type'       : df['device_type'][idx],
                'device_id'         : df['device_id'][idx],
                'time_diff[sec]'    : delta,  
                'beats_sec'         : 0,
                'beats_min'         : 0,
                'beats_hr'          : 0,
                'test_id'           : df['test_id'][idx],
                'ongoing'           : df['ongoing'][idx]
            }

            gap_rows.append(new_row)

        gap_rows_df = pd.DataFrame(gap_rows)
        new_df = pd.concat([df, gap_rows_df], ignore_index=True)
        new_df.sort_values(by='time_column', inplace=True, ignore_index=True)

        # Recalculate time_diff
        time_diff = Analysis.recalculate_time_diff(new_df)
        new_df['time_diff[sec]'] = time_diff

        return new_df

    
    @staticmethod
    def recalculate_time_diff(df: pd.DataFrame) -> pd.Series:
        """
        Recalculate the time differences between consecutive timestamps in the DataFrame.

        Parameters:
            df (DataFrame): The DataFrame containing the time data.

        Returns:
            Series: The recalculated time differences.
        """
        time_diff = pd.to_datetime(df['time_column'], format='%H:%M:%S').diff().dt.total_seconds().dropna(ignore_index=True)
        time_diff[len(time_diff)] = 0.0  # Adding zero to the end, since there is no time difference after that point.
        return time_diff
    
    
    def check_ongoing_test(self, df: pd.DataFrame) -> int:
        """
        Check whether there is an open test and find the index in the dataframe where it starts.

        Parameters:
            df (DataFrame): The DataFrame.

        Returns:
            last_index (int) or None. 

        """
        find_last_ending_df = df[df['log_code'].isin(self.end_code)]
        last_index = find_last_ending_df.index[-1] if not find_last_ending_df.empty else None
        
        if last_index:
            if last_index == df.shape[0]-1:
                return None
            else:
                return last_index + 1
        else:
            # If no last_index, meaning all the df is an ongoing test
            last_index = 0
            return last_index