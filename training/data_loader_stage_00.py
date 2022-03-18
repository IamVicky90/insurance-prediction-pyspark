from spark_manager.spark_manager import Spark_Manager
from insurance_exception.insurance_exception import InsuranceException as DataLoaderException
from utility import read_params, get_log_object_for_training
import argparse
import sys
import ast
log_collection_name = "Data_Loader"
class Data_Loader:
    def __init__(self, is_log_enabled: bool,log_collection_name: str = log_collection_name):
        """Responsible For Loading the Training Data.

        Args:
            log_collection_name (str): Name of the collection in MongoDb database.
            is_log_enabled (bool): If it is set to True then only it will write the logs. Defaults to True.
        """
        self.log_collection_name = log_collection_name
        self.logger = get_log_object_for_training(
            log_collection_name, is_log_enabled=is_log_enabled)
        self.ss=Spark_Manager().get_SparkSession()
    def load_data_from_path(self,path):
        """Load Data From the given path using Spark DataFrame.
        Args:
            path (str): Path of the data.
        Returns:
            SparkSeeion.DataFrame: Spark DataFrame
        """
        try:
            self.logger.log("Loading data from path has been started")
            df= self.ss.read.csv(path,header=True)
            self.logger.log(f"Sucessfully Load the data from path {path}")
            return df
        except Exception:
            error = DataLoaderException("Error in module {0} class {1} method {2}".format(
                Data_Loader.__module__, Data_Loader.__class__.__name__, self.load_data_from_path.__name__), sys)
            raise error

if __name__ == "__main__":
    args=argparse.ArgumentParser()
    args.add_argument("--log_collection_name", default=log_collection_name)
    args.add_argument("--is_log_enabled", default=True)
    args.add_argument(
        "--data_source", default=["data/training_batch_files/HealthPrem_26092020_131534.csv"])
    parsed_args=args.parse_args()
    if (parsed_args.is_log_enabled) == str:
        parsed_args.is_log_enabled = ast.literal_eval(
            parsed_args.is_log_enabled.title())
    logger=get_log_object_for_training(log_collection_name)
    data_loader = Data_Loader(parsed_args.is_log_enabled,
        log_collection_name)
    df = data_loader.load_data_from_path(parsed_args.data_source)


    
        