import subprocess

class Setup():
    
    # installing missing required packages
    @staticmethod
    def check_and_install_packages(packages):
        for package in packages:
            
            try:
                __import__(package)
                print(f"{package} is already installed")
           
            except ImportError:
                print(f"{package} is not installed. Installing...")
                subprocess.check_call(['python', '-m', 'pip', 'install', '-U', package])
                print(f"{package} has been installed")