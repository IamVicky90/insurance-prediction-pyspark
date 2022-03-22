import utility
from mongo_db import mongo_db_atlas
from insurance_exception.insurance_exception import InsuranceException as Export_Data_Exception
import argparse
import ast
import os
import sys
log_collection_name = 'export_data'
class export_data:
    def __init__(self,logger, config):
        try:
            self.__logger = logger
            self.__config = config
            self.__db = mongo_db_atlas.Mongo_operations()
        except Exception:
            error = Export_Data_Exception("Error in module {0} class {1} method {2}".format(
                export_data.__module__.__str__, export_data.__class__.__name__, "__init__"), sys)
            raise error

    def convert_mongo_data_to_master_csv(self, db_name, collection_name, path, master_csv_filename):
        """Responsible to converting the mongo_db data to the master csv

        Args:
            db_name (str): Name of the database that you want to convert.
            collection_name (str): Name of the collection name inside the database.
            path (str): Path to save the master csv.
        """
        try:
            self.__logger.log("inside convert_mongo_data_to_master_csv funtion")
            df = self.__db.get_data_from_mongo_db(db_name, collection_name)
            self.__logger.log("dataframe extracted")
            df.to_csv(os.path.join(path,master_csv_filename))
        except Exception:
            error = Export_Data_Exception("Error in module {0} class {1} method {2}".format(
                export_data.__module__.__str__, export_data.__class__.__name__, self.convert_mongo_data_to_master_csv.__name__), sys)
            raise error


def validate_data_exporter(master_csv_path, master_csv_filename, log_collection_name, config, is_log_enabled):
    utility.create_directory(master_csv_path)
    logger = utility.get_log_object_for_training(
        collection_name=log_collection_name, is_log_enabled=is_log_enabled)
    logger.log("Validate Data Exporter started....")
    config=utility.read_params(config)
    db_name = config['database_for_data']['database_name']
    collection_name =config['database_for_data']['collection_name']
    logger.log(f"Database name is {db_name}, Collection name is {collection_name}....")
    data_exporter=export_data(logger,config)
    logger.log("export_data funtion Executed Sucessfully....")
    data_exporter.convert_mongo_data_to_master_csv(
        db_name, collection_name, master_csv_path, master_csv_filename)
    logger.log("convert_mongo_data_to_master_csv funtion Executed Sucessfully....")

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument(
        "--config", default=os.path.join("config", "params.yaml"))
    args.add_argument("--master_csv_path",
                      default=utility.read_params()["artifacts"]["Data_Directories"]["training"]["master_csv_path"])
    args.add_argument("--master_csv_filename",
                      default=utility.read_params()["artifacts"]["Data_Directories"]["training"]["master_csv_filename"])
    args.add_argument("--log_collection_name", default=log_collection_name)
    args.add_argument("--is_log_enabled", default=True)
    parsed_args = args.parse_args()
    if (parsed_args.is_log_enabled) == str:
        parsed_args.is_log_enabled = ast.literal_eval(
            parsed_args.is_log_enabled.title())
    validate_data_exporter(master_csv_path=parsed_args.master_csv_path, master_csv_filename=parsed_args.master_csv_filename, log_collection_name=parsed_args.log_collection_name,
                                 config=parsed_args.config, is_log_enabled=parsed_args.is_log_enabled)


