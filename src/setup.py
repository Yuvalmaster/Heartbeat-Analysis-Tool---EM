import subprocess

class Setup():
    
    @staticmethod
    def check_and_install_packages(requirements_file):
        print('\nCHECK IF ALL DEPENDENCIES EXISTS\n\n')
        try:
            subprocess.run(['pip', 'install', '-r', requirements_file], check=True)
            print('\nAll packages installed successfully!\n\n')
        except subprocess.CalledProcessError as error:
            print(f"\n\An error occurred while installing libraries: {error}\n")
                

