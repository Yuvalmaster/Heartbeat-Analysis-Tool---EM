import os
from setup import Setup

# Project folders
src_dir     = os.getcwd()                                                              # code path
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))              # Project path
data_dir    = os.path.join(project_dir, 'data')                                        # data path
config_dir  = os.path.join(project_dir, 'config')                                      # config path

# Check if 'requirements.txt' exists and install packages
requirements_file = os.path.join(project_dir, 'requirements.txt')
if os.path.exists(requirements_file):
    Setup.check_and_install_packages(requirements_file)

from data_loader import DataLoader
def main():
    # load raw data to database
    dl = DataLoader(data_directory=data_dir, config_directory=config_dir)
       
    # load database object
    db = dl.db
    
    # run analysis
    dl.run_analysis_for_all_devices()
    
    db.close_db()
        
if __name__ == "__main__":
    main()