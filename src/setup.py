import subprocess

class Setup():
    
    @staticmethod
    def check_and_install_packages(requirements_file):
        try:
            subprocess.run(['pip', 'install', '-r', requirements_file], check=True)
            print('\nAll packages installed successfully!\n\n')
        except subprocess.CalledProcessError:
            print("An error occurred while installing libraries.")
                

