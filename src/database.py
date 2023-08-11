import psycopg2
import pandas as pd

class Database:
    def __init__(self, db_config: dict):
        """
        Initialize the Database class.

        Parameters:
            db_config (dict): Dictionary containing database configuration details.
        """
        self.connection = self.connect_database(db_config=db_config)
        self.cursor = self.connection.cursor()

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
                print(f"Database '{db_config['database']}' created successfully.")
            else:
                print(f"Database '{db_config['database']}' already exists.")

        except psycopg2.Error as error:
            print(f"Error creating or checking database: {error}")

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
        schema_name = 'raw_data'
        table_name = data['Title']
        df = data['DataFrame']

        try:
            create_schema_query = self.build_create_schema(schema_name)
            self.cursor.execute(create_schema_query)
            self.connection.commit()

            create_table_query = self.build_create_table_query(schema_name, table_name, df)
            self.cursor.execute(create_table_query)
            self.connection.commit()

            # Prevent insertion of the same data to the table in the database
            exist = self.check_file_in_table(schema_name, table_name, data['DATE'])
            if exist:
                return

            # Convert DataFrame to a list of tuples for bulk insertion
            records_to_insert = df.astype(str).to_records(index=False)
            records_to_insert = [tuple(record) for record in records_to_insert]

            # Insert data to the table
            insert_query = self.build_insert_query(schema_name, table_name, df)
            self.cursor.executemany(insert_query, records_to_insert)
            self.connection.commit()
            
        except psycopg2.Error as error:
            self.connection.rollback()
            print(f"Error saving data to PostgreSQL: {error}")


    def save_device(self, device: str, id: str):
        """
        Save device data to the database if it doesn't exist.

        Parameters:
            device (str): DEVICE_TYPE
            id (str): DEVICE_ID

        Returns:
            None
        """
        table_name = 'devices'
        try:
            self.create_devices_table(table_name)
            
            insert_query = f'''
                INSERT INTO {table_name} (Device, ID)
                SELECT %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} WHERE Device = %s AND ID = %s
                );
            '''
            self.cursor.execute(insert_query, (device.lower(), id, device.lower(), id))
            self.connection.commit()

        except psycopg2.Error as error:
            self.connection.rollback()
            print(f"Error saving unique device data to database: {error}")
    
    
    def save_analysis_data(self, df: pd.DataFrame, table_name: str, analysis_type: str):
        """
        Save analysed DataFrame to the database.

        Parameters:
            df (DataFrame): Analyzed data.
            table_name (str): Name of the table.
            analysis_type (str): Type of analysis.

        Returns:
            None
        """   
        valid_types = ['rate', 'total']
        if analysis_type not in valid_types:
            print('Invalid analysis type')
            return
        
        table_name = f'{table_name}_analysis_{analysis_type}'
        schema_name = f'{analysis_type}_analysis_data'

        try:
            create_schema_query = self.build_create_schema(schema_name)
            self.cursor.execute(create_schema_query)
            self.connection.commit()
            
            create_table_query = self.build_create_table_query(schema_name, table_name, df)
            self.cursor.execute(create_table_query)
            self.connection.commit()

            # Convert DataFrame to a list of tuples for bulk insertion
            records_to_insert = df.astype(str).to_records(index=False)
            records_to_insert = [tuple(record) for record in records_to_insert]

            # Insert data to temp table to add only new records to the existing analysis table
            temp_table_name = f"temp_{table_name}"
            temp_table_query = self.build_create_table_query(schema_name, temp_table_name, df)
            self.cursor.execute(temp_table_query)
            self.connection.commit()
            
            insert_query = self.build_insert_query(schema_name, temp_table_name, df)
            self.cursor.executemany(insert_query, records_to_insert)
            self.connection.commit()
            
            insert_query = f'''
                INSERT INTO {schema_name}.{table_name}
                SELECT temp.* FROM {schema_name}.{temp_table_name} temp
                LEFT JOIN {schema_name}.{table_name} analysis 
                USING ({', '.join(df.columns)})
                WHERE analysis.{df.columns[0]} IS NULL;
            '''
            self.cursor.execute(insert_query)
            self.connection.commit()

        except psycopg2.Error as error:
            self.connection.rollback()
            print(f"Error saving analysis data to database: {error}")
            
        finally:
            # Drop the temporary table
            drop_temp_table_query = f"DROP TABLE IF EXISTS {schema_name}.{temp_table_name}"
            self.cursor.execute(drop_temp_table_query)
            self.connection.commit()
            

    # %% QUERY BUILDERS
    
    def build_create_schema(self, schema_name:str) -> str:
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
            schema_name (str): The name of the schema
            table_name (str): The name of the table to be created.
            df (DataFrame): DataFrame to infer the column names and types.

        Returns:
            str: SQL query to create the new table.
        """
        columns = ", ".join(f"{column} {self.get_sql_type(df[column])}" for column in df.columns)
        create_table_query = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns})"
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
    

    # %% ADDITIONAL TOOLS   
             
    def check_file_in_table(self, schema_name:str, table_name: str, date: str) -> bool:
        """
        Check if a file exists in a specific table for a given date.

        Parameters:
            schema_name (str): Name of the schema.
            table_name (str): Name of the table.
            date (str): Date to check.

        Returns:
            bool: True if the date exists, False otherwise.
        """
        # Execute the SQL query to check if the specific date exists in the table
        query = f"""
            SELECT EXISTS (
                SELECT 1 FROM {schema_name}.{table_name}
                WHERE time_column::date = '{date}'
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
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'raw_data'
            """

            self.cursor.execute(query)
            table_names = [row[0] for row in self.cursor.fetchall()]

            return table_names

        except psycopg2.Error as error:
            print(f"Error fetching table names: {error}")
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
        create_table_query = f'''CREATE TABLE IF NOT EXISTS {table_name} (
                                    Device VARCHAR(255),
                                    ID VARCHAR(255)
                                    )'''
                                    
        self.cursor.execute(create_table_query)
        self.connection.commit()
    

