from pyspark.ml import PipelineModel
from pyspark.ml.regression import RandomForestRegressionModel
import utility
from spark_manager.spark_manager import Spark_Manager
from mongo_db.mongo_db_atlas import Mongo_operations
import argparse
import ast
import sys
import os
from insurance_exception.insurance_exception import InsuranceException as Batch_Prediction_Exception
log_collection_name="Batch Prediction"
class Batch_Prediction:
    def __init__(self, master_csv_path, master_csv_filename, config, logger):
        """Responsible for batch prediction

        Args:
            master_csv_path (str): Master Csv file path.
            master_csv_filename (str): Master Csv File Name.
            config (object): Configuration file.
            logger (object): To log the results
        """
        try:
            self.__config=config
            self.__log=logger
            self.__master_csv_path = master_csv_path
            self.__master_csv_filename = master_csv_filename
            self.__ss=Spark_Manager().get_SparkSession()
            self.__db=Mongo_operations()
            self.__collection_name = self.__config["prediction_data"]["collection_name"]
            self.__database_name = self.__config["database_for_data"]["database_name"]
            self.__log.log("Inside __init__ method of Batch_Prediction")
        except Exception:
            error = Batch_Prediction_Exception("Error in module {0} class {1} method {2}".format(
                Batch_Prediction.__module__.__str__, Batch_Prediction.__class__.__name__, "__init__"), sys)
            raise error
    def get_prediction(self):
        """Give us the prediction in the form of Spark.DataFrame

        Returns:
            Spark.DataFrame: Dataframe of Pyspark.
        """
        try:
            pipelining = PipelineModel.load('artifacts/pipelining')
            self.__log.log("pipelining loaded successfully")
            model = RandomForestRegressionModel.load(
                'artifacts/training_data/model/random_forest_regressor')
            self.__log.log("RandomForestRegressionModel loaded successfully")
            dataframe=self.get_dataframe(os.path.join(self.__master_csv_path,self.__master_csv_filename))
            process_data = pipelining.transform(dataframe)
            predict_dataframe = model.transform(process_data)
            self.__log.log("DataFrame Transformed successfully")
            print(predict_dataframe.show())
            print(predict_dataframe.columns)
            df=predict_dataframe.select('age', 'sex', 'bmi',
                'children', 'smoker','prediction')
            return df
        except Exception:
            error = Batch_Prediction_Exception("Error in module {0} class {1} method {2}".format(
                Batch_Prediction.__module__, Batch_Prediction.__class__.__name__, self.get_prediction.__name__), sys)
            raise error
    def get_dataframe(self,dataframepath):
        """Responsible for giving the dataframe

        Args:
            dataframepath (str): Path of the dataframe

        Returns:
            Spark.DataFrame: Spark.DataFrame
        """
        try:
            return self.__ss.read.csv(dataframepath,inferSchema=True,header=True)
        except Exception:
            error = Batch_Prediction_Exception("Error in module {0} class {1} method {2}".format(
                Batch_Prediction.__module__, Batch_Prediction.__class__.__name__, self.get_dataframe.__name__), sys)
            raise error
    def dump_data_into_mongodb(self,df):
        """Will dump the data into Mongodb Atlas.

        Args:
            df (Spark.DataFrame): DataFrame of Pyspark
        """
        try:
            self.__db.insert_many_records(records=df.toPandas().T.to_dict().values(
                    ), dbname=self.__database_name,collection_name= self.__collection_name)
            self.__log.log("Successfully dump data into mongodb Atlas")
        except Exception:
            error = Batch_Prediction_Exception("Error in module {0} class {1} method {2}".format(
                Batch_Prediction.__module__, Batch_Prediction.__class__.__name__, self.dump_data_into_mongodb.__name__), sys)
            raise error


def validate_data_prediction(master_csv_path, master_csv_filename, log_collection_name, config, is_log_enabled=True):
    """Responsible for executing the pipeline

    Args:
        master_csv_path (str): Master Csv file path.
        master_csv_filename (str): Master Csv File Name.
        config (object): Configuration file.
        logger (object): To log the results
        is_log_enabled (bool): If True then only the information will be logged. Defaults to True.
    """
    logger = utility.get_log_object_for_prediction(
        collection_name=log_collection_name, is_log_enabled=is_log_enabled)
    logger.log("Validate Data Prediction started....")
    config = utility.read_params(config)
    bp = Batch_Prediction(master_csv_path,
                          master_csv_filename, config, logger)
    logger.log("Batch_Prediction function run successfully....")
    df=bp.get_prediction()
    logger.log("get_prediction function run successfully....")
    bp.dump_data_into_mongodb(df)
    logger.log("dump_data_into_mongodb function run successfully....")
if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument(
        "--config", default=os.path.join("config", "params.yaml"))
    args.add_argument("--master_csv_path",
                      default=utility.read_params()["artifacts"]["Data_Directories"]["prediction"]["master_csv_path"])
    args.add_argument("--master_csv_filename",
                      default=utility.read_params()["artifacts"]["Data_Directories"]["prediction"]["master_csv_filename"])
    args.add_argument("--log_collection_name", default=log_collection_name)
    args.add_argument("--is_log_enabled", default=True)
    parsed_args = args.parse_args()
    if (parsed_args.is_log_enabled) == str:
        parsed_args.is_log_enabled = ast.literal_eval(
            parsed_args.is_log_enabled.title())
    validate_data_prediction(master_csv_path=parsed_args.master_csv_path, master_csv_filename=parsed_args.master_csv_filename, log_collection_name=parsed_args.log_collection_name,
                           config=parsed_args.config, is_log_enabled=parsed_args.is_log_enabled)
