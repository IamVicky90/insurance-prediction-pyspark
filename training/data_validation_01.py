import re
import shutil
from insurance_exception.insurance_exception import InsuranceException
import sys
import utility
import os
class Data_Validation:
    def __init__(self,collection_name: str,config,is_log_enabled : bool=True):
    # def __init__(self,logger,collection_name: str,config,is_log_enabled : bool=True):
        """Responsible for Data Validation.

        Args:
            logger (logger_obj): Object of the logger.
            collection_name (str): Name of the mongodb collection to store logs.
            is_log_enabled (bool, optional): If True then only the loggings will be store. Defaults to True.
        """
        self.__collection_name = collection_name
        self.__is_log_enabled = is_log_enabled
        # self.__logger=logger
        self.__config=config
    def return_regular_expression(self):
        """Return the regular expression for the data files to make sure that it only validates the right files
        """
        return "[HealthPrem_]+[0-9]+[_]+[0-9]+.csv"
    def move_to_bad_archive_directory(self,folder_path,file_name:str):
        """Move the bad file to Bad Archive Directory

        Args:
            file_path (str): Path of the file
        """
        try:
            #problem
            self.create_bad_data_directory(folder_path)
            shutil.move(
                os.path.join(folder_path, file_name), self.__configself.__config['artifacts']['Data_Directories']['training']['Bad_Data_Directory'])
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Data_Validation.__module__.__str__, Data_Validation.__class__.__name__, self.move_to_bad_archive_directory.__name__), sys)
            raise error

    def copy_to_good_data_directory(self, folder_path, file_name: str):
        """Copy the good file to Good Data Directory

        Args:
            file_path (str): Path of the file
        """
        try:
            self.create_good_data_directory(folder_path)
            shutil.copy(
                os.path.join(folder_path, file_name), self.__config['artifacts']['Data_Directories']['training']['Good_Data_Directory'])
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Data_Validation.__module__.__str__, Data_Validation.__class__.__name__, self.copy_to_good_data_directory.__name__), sys)
            raise error
    def create_good_data_directory(self,path):
        """Responsible for creating Good Data directory

        Args:
            path (str): path of the Good Data directory
        """
        utility.create_directory(path)
    def create_bad_data_directory(self,path):
        """Responsible for creating Bad Data directory

        Args:
            path (str): path of the Bad Data directory
        """
        utility.create_directory(path)
    def validate_file_name(self,batch_directory_path):
        files = os.listdir(batch_directory_path)
        LengthOfDateStampInFile, LengthOfTimeStampInFile, _, _ = utility.read_training_schema()
        match_string = self.return_regular_expression()
        for file_name in files:
            if not file_name.endswith('.csv'):
                continue
            datestamp_length = len(file_name.split('_')[1])
            timestamp_length = len(file_name.split('_')[2].split('.csv')[0])
            if re.match(match_string, file_name) and datestamp_length == LengthOfDateStampInFile and timestamp_length == LengthOfTimeStampInFile:
                self.copy_to_good_data_directory(os.path.join(
                    os.getcwd(), batch_directory_path), file_name)
                print('copy')
            else:
                self.move_to_bad_archive_directory(
                    os.path.join(os.getcwd(), batch_directory_path), file_name)
                print('move')


obj = Data_Validation('test2', utility.read_params())
obj.validate_file_name('data/training_batch_files')
    
