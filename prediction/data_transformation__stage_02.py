import utility
import os
from spark_manager.spark_manager import Spark_Manager
from mongo_db import mongo_db_atlas
import ast
import argparse
import sys
from insurance_exception.insurance_exception import InsuranceException as Transformation_Exception
log_collection_name = 'Data_Transformation'
class Data_Transformation:
    def __init__(self, logger, config):
        """Responsible for Data TRansformation.

        Args:
            logger (logger_obj): Object of the logger.
            config (str): path of the configuration file
        """
        try:
            self.__logger = logger
            self.__config = utility.read_params(config)
            self.__db=mongo_db_atlas.Mongo_operations()
            self.__logger.log("Data_Transformation started....")
            self.ss = Spark_Manager().get_SparkSession()
        except Exception:
            error = Transformation_Exception("Error in module {0} class {1} method {2}".format(
                Data_Transformation.__module__.__str__, Data_Transformation.__class__.__name__, "__init__"), sys)
            raise error
    def unite_data(self):
        """Responsible to combine all the batch training data into one sqllite db
        """
        try:
            path = self.__config["artifacts"]["Data_Directories"]\
                                                    ["prediction"]\
                                            ["Good_Data_Directory"]
            files=os.listdir(path)
            i=0
            j=0
            for file in files:
                if file.endswith(".csv"):
                        if i==0:
                            df=self.ss.read.csv(os.path.join(path, file),header=True)
                            i=i+1
                            final_df = df
                            if j==1:
                                total_df = total_df.union(df)
                                df=total_df
                                final_df = total_df
                        else:
                            j=1
                            total_df = self.ss.read.csv(
                                os.path.join(path, file), header=True)
                            df = df.union(total_df)
                            total_df = df
                            i=i-1
                            final_df=df
            return final_df
        except Exception:
            error = Transformation_Exception("Error in module {0} class {1} method {2}".format(
                Data_Transformation.__module__.__str__, Data_Transformation.__class__.__name__, self.unite_data.__name__), sys)
            raise error
    def remove_unwanted_column(self,df):
        """Remove the column that you donot want...

        Args:
            df (spark.data.DataFrame): expected a spark dataframe.

        Returns:
            spark.data.DataFrame: Return the spark dataframe.
        """
        try:
            drop_cols=self.__config["data"]['unwanted_columns']
            for col in drop_cols:
                df_ = df.drop(col)
                df=df_
            return df
        except Exception:
            error = Transformation_Exception("Error in module {0} class {1} method {2}".format(
                Data_Transformation.__module__.__str__, Data_Transformation.__class__.__name__, self.remove_unwanted_column.__name__), sys)
            raise error
    def import_data_to_mongodb(self,df):
        """Import Data to Mongodb Atlas

        Args:
            df (spark.data.DataFrame): expected a spark dataframe.
        """
        try:
            dbname=self.__config['database_for_prediction_data']['database_name']
            collection_name=self.__config['database_for_prediction_data']['collection_name']
            self.__db.insert_many_records(records=df.toPandas().T.to_dict().values(
            ), dbname=dbname, collection_name=collection_name)
        except Exception:
            error = Transformation_Exception("Error in module {0} class {1} method {2}".format(
                Data_Transformation.__module__.__str__, Data_Transformation.__class__.__name__, self.import_data_to_mongodb.__name__), sys)
            raise error
def validate_data_transformation(collection_name, config, is_log_enabled):
    logger = utility.get_log_object_for_prediction(
        collection_name=collection_name, is_log_enabled=is_log_enabled)
    logger.log("Validate main started....")
    dt = Data_Transformation(
        logger, config)
    logger.log("Data_Transformation funtion successfully completed....")
    final_df = dt.unite_data()
    logger.log("unite_data funtion successfully completed....")
    df = dt.remove_unwanted_column(final_df)
    logger.log("remove_unwanted_column funtion successfully completed....")
    dt.import_data_to_mongodb(df)
    logger.log("import_data_to_mongodb funtion successfully completed....")
if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument(
        "--config", default=os.path.join("config", "params.yaml"))
    args.add_argument("--log_collection_name", default=log_collection_name)
    args.add_argument("--is_log_enabled", default=True)
    parsed_args = args.parse_args()
    if (parsed_args.is_log_enabled) == str:
        parsed_args.is_log_enabled = ast.literal_eval(
            parsed_args.is_log_enabled.title())
    validate_data_transformation(collection_name=parsed_args.log_collection_name,
                                 config=parsed_args.config, is_log_enabled=parsed_args.is_log_enabled) 






    