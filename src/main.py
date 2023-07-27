# Required packages for the system
REQUIRED_PACKAGES = ['numpy', 'pandas', 'matplotlib', 'tqdm', 'yaml','psycopg2']    
from setup import Setup
Setup.check_and_install_packages(REQUIRED_PACKAGES)

# import packages
import os
from data_loader import DataLoader

# Project folders
src_dir     = os.getcwd()                                                              # code path
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))              # Project path
data_dir    = os.path.join(project_dir, 'data')                                        # data path
config_dir  = os.path.join(project_dir, 'config')                                      # config path

def main():
   
    # load raw data to database
    dl = DataLoader(data_directory=data_dir, config_directory=config_dir)
    
    # load database object
    db = dl.db
    
    # run analysis
    dl.run_analysis_for_all_devices(db, config_dir)
    
    db.close_db()
        
if __name__ == "__main__":
    main()