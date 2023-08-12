import psycopg2
import pandas as pd
from sqlalchemy import create_engine

class Database:
    def __init__(self, db_config: dict):
        """
        Initialize the Database class.

        Parameters:
            db_config (dict): Dictionary containing database configuration details.
        """
        self.connection = self.connect_database(db_config=db_config)
        self.cursor = self.connection.cursor()
        self.schema_name = db_config['schema']
        self.table_names = db_config['table_names']
        
        ############################################################
        # NOTE: All tables are under a unified schema. The save data
        # tables are: 
        #   1.'devices' 
        #   2.'raw_data' 
        #   3.'analysis_total_beats_over_time' 
        #   4.'analysis_rate_over_time'
        # 
        # OPTIONAL: It may be recommended, order-wise, to split the
        # analysis tables to a different schema. Additionally, 
        # since the database can hold thousands of devices with 
        # milions of record each, it may be rquired to partition the
        # raw_data and analysis tables and/or save them in different
        # schemas. 
        ############################################################


    # %% DATABASE CONNECTION 
       
    def connect_database(self, db_config: dict):
        """
        Connect to the database and create a new database if it doesn't exist.

        Parameters:
            db_config (dict): Dictionary containing database configuration details.

        Returns:
            connection: Database connection object.
        """
        # Connect to the default 'postgres' database
        default_connection = psycopg2.connect(
            host     = db_config['host'],
            port     = db_config['port'],
            database = 'postgres',
            user     = db_config['user'],
            password = db_config['password']
        )

        default_connection.set_isolation_level(0)  # Set isolation level to Autocommit
        default_cursor = default_connection.cursor()

        # Check if the database exists. If not exists, create a new database
        try:
            default_cursor.execute("SELECT datname FROM pg_database;")
            databases = default_cursor.fetchall()
            exists = any(db_config['database'] in db for db in databases)

            if not exists:
                # Create the database if it doesn't exist
                default_cursor.execute(f'CREATE DATABASE "{db_config["database"]}";')
                print(f"Database -> connect_database: Database '{db_config['database']}' created successfully.")

        except psycopg2.Error as error:
            print(f"Database -> connect_database: Error creating or checking database: {error}")

        finally:
            default_cursor.close()
            default_connection.close()
            db_connection = psycopg2.connect(
                host     = db_config['host'],
                port     = db_config['port'],
                database = db_config['database'],
                user     = db_config['user'],
                password = db_config['password']
            )

            return db_connection

        
    def close_db(self):
        """
        Close the database connection.

        Returns:
            None
        """
        self.cursor.close()
        self.connection.close()
        

    # %% SAVE DATA TO DATABASE 
    
    def save_raw_data(self, data: dict) -> None:
        """
        Save raw data to the database.

        Parameters:
            data (dict): Dictionary containing the data to be saved.

        Returns:
            None
        """
        table_name = self.table_names[1]
        df = data['DataFrame']

        try:
            create_schema_query = self.build_create_schema_query(self.schema_name)
            self.cursor.execute(create_schema_query)
            self.connection.commit()

            create_table_query = self.build_create_table_query(self.schema_name, table_name, df)
            self.cursor.execute(create_table_query)
            self.connection.commit()

            # Prevent insertion of the same data to the table in the database
            exist = self.check_file_in_table(self.schema_name, table_name, data['DATE'], data['DEVICE_TYPE'], data['DEVICE_ID'])
            if exist:
                return

            # Convert DataFrame to a list of tuples for bulk insertion
            records_to_insert = df.astype(str).to_records(index=False)
            records_to_insert = [tuple(record) for record in records_to_insert]

            # Insert data to the table
            insert_query = self.build_insert_query(self.schema_name, table_name, df)
            self.cursor.executemany(insert_query, records_to_insert)
            self.connection.commit()
            
        except psycopg2.Error as error:
            self.connection.rollback()
            print(f"Database -> save_raw_data: Error saving data to PostgreSQL: {error}")


    def save_device(self, device: str, id: str):
        """
        Save device data to the database if it doesn't exist.

        Parameters:
            device (str): DEVICE_TYPE
            id (str): DEVICE_ID

        Returns:
            None
        """
        table_name = self.table_names[0]
        try:
            self.create_devices_table(table_name)
            
            insert_query = f'''
                INSERT INTO {self.schema_name}.{table_name} (device_type, device_id)
                SELECT %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {self.schema_name}.{table_name} 
                    WHERE device_type = %s AND device_id = %s
                );
            '''
            self.cursor.execute(insert_query, (device.lower(), id, device.lower(), id))
            self.connection.commit()

        except psycopg2.Error as error:
            self.connection.rollback()
            print(f"Database -> save_device: Error saving unique device data to database: {error}")
    
    
    def save_analysis_data(self, df: pd.DataFrame, schema_name:str, analysis_type: str):
        """
        Save analysed DataFrame to the database.

        Parameters:
            df (DataFrame): Analyzed data.
            schema_name (str): Name of the schema.
            analysis_type (str): Type of analysis.

        Returns:
            None
        """

        if analysis_type not in self.table_names[2:]:
            print('Database -> save_analysis_data: Invalid analysis type')
            return
        
        try:
            # If schema not created for some reason, or whether the table saves to a different schema.
            create_schema_query = self.build_create_schema_query(schema_name)
            self.cursor.execute(create_schema_query)
            self.connection.commit()
            
            create_table_query = self.build_create_table_query(schema_name, analysis_type, df)
            self.cursor.execute(create_table_query)
            self.connection.commit()
            
            query = f'''
            DELETE FROM {self.schema_name}.{analysis_type}
            WHERE ongoing = 'True';
            '''  
            self.cursor.execute(query)
            self.connection.commit()  

            # Convert DataFrame to a list of tuples for bulk insertion
            records_to_insert = df.astype(str).to_records(index=False)
            records_to_insert = [tuple(record) for record in records_to_insert]
            
            insert_query = self.build_insert_query(schema_name, analysis_type, df)
            self.cursor.executemany(insert_query, records_to_insert)
            self.connection.commit()

        except psycopg2.Error as error:
            self.connection.rollback()
            print(f"Database -> save_analysis_data: Error saving analysis data to database: {error}")
         

    # %% QUERY BUILDERS
    
    def build_create_schema_query(self, schema_name:str) -> str:
        """
        Build SQL query to create a new schema.

        Parameters:
            schema_name (str): The name of the schema to be created.

        Returns:
            str: SQL query to create the new schema.
        """
        create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        return create_schema_query
    
    
    def build_create_table_query(self, schema_name:str, table_name: str, df: pd.DataFrame) -> str:
        """
        Build the SQL query to create a new table based on the DataFrame columns.

        Parameters:
            schema_name (str): The name of the schema.
            table_name (str): The name of the table to be created.
            df (DataFrame): DataFrame to infer the column names and types.

        Returns:
            str: SQL query to create the new table.
        """
        columns = ", ".join(f"{column} {self.get_sql_type(df[column])}" for column in df.columns)
        create_table_query = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (id_primary SERIAL PRIMARY KEY, {columns})"
        return create_table_query


    def build_insert_query(self, schema_name:str, table_name: str, df: pd.DataFrame) -> str:
        """
        Build the SQL query to insert data into the table based on the DataFrame columns.

        Parameters:
            schema_name (str): Name of the schema.
            table_name (str): Name of the table.
            df (DataFrame): DataFrame to infer the column names.

        Returns:
            str: SQL query to insert data into the table.
        """
        columns = ", ".join(df.columns)
        placeholders = ", ".join("%s" for _ in df.columns)
        insert_query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders})"
        return insert_query
    

    # %% UTILITIES & ADDITIONAL TOOLS
             
    def check_file_in_table(self, schema_name:str, table_name: str, date: str, device_type: str, device_id: str) -> bool:
        """
        Check if a file exists in a specific table for a given date.

        Parameters:
            schema_name (str): Name of the schema.
            table_name (str): Name of the table.
            date (str): Date to check.
            device_type (str): Device type to check.
            device_id (str): Device ID to check.

        Returns:
            bool: True if the date exists, False otherwise.
        """
        # Execute the SQL query to check if the specific date exists in the table
        query = f"""
            SELECT EXISTS (
                SELECT 1 FROM {schema_name}.{table_name}
                WHERE 
                    time_column::date = '{date}' AND
                    device_type = '{device_type}' AND
                    device_id = '{device_id}'
            );
        """
        self.cursor.execute(query)

        # Fetch the result
        exists = self.cursor.fetchone()[0]
        return exists
    
    
    def find_all_devices(self) -> list:
        """
        Find all unique devices in the database by table name.

        Returns:
            list: List of all unique table names in the database.
        """
        try:
            # SQL query to fetch all table names
            query = """
                SELECT device_type, device_id 
                FROM devices_and_analysis.devices
                ORDER BY id_primary ASC 
            """

            self.cursor.execute(query)
            devices = self.cursor.fetchall()

            return devices

        except psycopg2.Error as error:
            print(f"Database -> find_all_devices: Error fetching table names: {error}")
            return None    
        
            
    def get_sql_type(self, column: pd.Series) -> str:
        """
        Infer the SQL data type based on the pandas DataFrame column.

        Parameters:
            column: A pandas DataFrame column.

        Returns:
            str: SQL data type.
        """
        # Map pandas data types to SQL data types
        sql_type_map = {
            'int64': 'INT',
            'float64': 'FLOAT',
            'object': 'VARCHAR(255)',
            'datetime64[ns]': 'TIMESTAMP',
        }

        pandas_type = str(column.dtype)
        return sql_type_map.get(pandas_type, 'VARCHAR(255)')
    
    
    def create_devices_table(self, table_name: str) -> str:
        """
        Create a devices table if it doesn't exist.

        Parameters:
            table_name (str): Name of the table.

        Returns:
            None
        """
        create_table_query = f'''CREATE TABLE IF NOT EXISTS {self.schema_name}.{table_name} (
                                    id_primary SERIAL PRIMARY KEY,
                                    device_type VARCHAR(255),
                                    device_id VARCHAR(255)
                                    )'''
                                    
        self.cursor.execute(create_table_query)
        self.connection.commit()
        
    
    def table_exists(self, schema_name, table_name):
        """
        Check if table exists in database.

        Parameters:
            schema_name (str): The schema name to search the table.
            table_name (str): Name of the searched table

        Returns:
            Bool: True when exists, False not exists.
        """
        query = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = '{schema_name}'
                AND table_name = '{table_name}'
            );
        """
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]
    
        
    # %% ANALYSIS DATA LOADER AND DB HANDLING
        
    def load_data_for_analysis(self, device: tuple, last_primary_id=None) -> pd.DataFrame:
        """
        Get required raw_data from database.

        Parameters:
            device (tuple): Tuple containing device type and device ID.
            last_primary_id (int): Last primary ID used for limiting the analysis to new data only.

        Returns:
            DataFrame: The DataFrame containing the required raw data from the database.
        """
        if last_primary_id:
        
            query = f"""
                SELECT * 
                FROM {self.schema_name}.{self.table_names[1]}
                WHERE
                    id_primary > {last_primary_id} AND 
                    device_type = '{device[0]}' AND
                    device_id = '{device[1]}' ;
            """    
        else:
            query = f"""
                SELECT * 
                FROM {self.schema_name}.{self.table_names[1]}
                WHERE
                    device_type = '{device[0]}' AND
                    device_id = '{device[1]}' ;
            """
        
        try:    
            # Get data from database
            engine = create_engine('postgresql+psycopg2://', creator=lambda: self.connection)
            df = pd.read_sql_query(query, engine)
            df = df.drop(columns=['id_primary'])
  
        except Exception as error:
            raise Exception(f"Database -> load_data_for_analysis: Error while fetching data from the database: {str(error)}")
    
        return df
    
    
    def check_last_test_id(self, device: tuple) -> int:
        """
        Check the last test ID for a given device.

        Parameters:
            device (tuple): Tuple containing device type and device ID.

        Returns:
            int: Last test ID.
        """
        table_exists = self.table_exists(self.schema_name, self.table_names[2])

        if table_exists:
            query_last_id = f"""
                SELECT DISTINCT test_id
                FROM {self.schema_name}.{self.table_names[2]}
                WHERE device_type = '{device[0]}' AND device_id = '{device[1]}'
                ORDER BY test_id DESC
                LIMIT 1;
            """
            self.cursor.execute(query_last_id)
            last_test_id = self.cursor.fetchone()
            
            if last_test_id is not None:
                last_test_id = last_test_id[0]
            else:
                last_test_id = 0
                
            return last_test_id
        else:
            return 0     