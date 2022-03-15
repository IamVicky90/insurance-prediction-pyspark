import re
import shutil
from insurance_exception.insurance_exception import InsuranceException
import sys
import utility
import os
import argparse
log_collection_name="data_validation"
class Data_Validation:
    def __init__(self,logger,config):
        """Responsible for Data Validation.

        Args:
            logger (logger_obj): Object of the logger.
            config (str): path of the configuration file
        """
        self.__logger=logger
        self.__config=utility.read_params(config)
        self.__logger.log("Data_Validation started....")
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
            Bad_Data_Directory = self.__config['artifacts']['Data_Directories']['training']['Bad_Data_Directory']
            destination_folder=os.path.join(Bad_Data_Directory,utility.get_date(),utility.get_time())
            self.create_bad_data_directory(destination_folder)
            shutil.move(
                os.path.join(folder_path, file_name), destination_folder)
            self.__logger.log(
                f"Successfully move the file {file_name} to {destination_folder}")
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Data_Validation.__module__, Data_Validation.__class__.__name__, self.move_to_bad_archive_directory.__name__), sys)
            raise error

    def copy_to_good_data_directory(self, folder_path, file_name: str):
        """Copy the good file to Good Data Directory

        Args:
            file_path (str): Path of the file
        """
        try:
            Good_Data_Directory = self.__config['artifacts']['Data_Directories']['training']['Good_Data_Directory']
            destination_folder = os.path.join(
                Good_Data_Directory, utility.get_date(), utility.get_time())
            self.create_good_data_directory(destination_folder)
            shutil.copy(
                os.path.join(folder_path, file_name), destination_folder)
            self.__logger.log(
                f"Successfully move the file {file_name} to {destination_folder}")
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Data_Validation.__module__, Data_Validation.__class__.__name__, self.copy_to_good_data_directory.__name__), sys)
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
        self.__logger.log("Filename validation started....")
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
                self.__logger.log(f"Filename {file_name} is match with the regex so moving to Good Data Directory path")
            else:
                self.move_to_bad_archive_directory(
                    os.path.join(os.getcwd(), batch_directory_path), file_name)
                self.__logger.log(f"Filename {file_name} is not match with the regex so moving to Bad Data Directory path")
def validation_main(datasource,collection_name,config,is_log_enabled):
    logger = utility.get_log_object_for_training(
        collection_name=collection_name, is_log_enabled=is_log_enabled)
    logger.log("Validate main started....")
    data_validation = Data_Validation(logger=logger, config=config)
    logger.log("Data Validation Run Successfully....")
    data_validation.validate_file_name(datasource)
    logger.log("validate_file_name funtion Run Successfully....")

if __name__=='__main__':
    args=argparse.ArgumentParser()
    args.add_argument("--config", default=os.path.join("config","params.yaml"))
    args.add_argument("--log_collection_name", default=log_collection_name)
    args.add_argument("--is_log_enabled", default=True)
    args.add_argument("--data_source", default=os.path.join(os.getcwd(), utility.read_params()['batch_file']['training_batch_files_path']))
    parsed_args=args.parse_args()
    validation_main(datasource=parsed_args.data_source,
                    collection_name=parsed_args.log_collection_name, config=parsed_args.config, is_log_enabled=parsed_args.is_log_enabled)
    
