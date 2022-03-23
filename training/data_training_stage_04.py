import utility
import os
from insurance_exception.insurance_exception import InsuranceException as Data_Preprocessing_Exception
from sklearn.metrics import r2_score, mean_squared_error
import numpy as np
from spark_manager.spark_manager import Spark_Manager
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql.types import IntegerType, StringType, FloatType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
import argparse
import ast
import sys
log_collection_name = "Training_model"
class Data_Preprocessing:
    def __init__(self, logger, dataframe, pipeline_path: str):
        """Responsible for Data_Preprocessing.

        Args:
            logger (logger_object): Object of the logger.
            is_log_enabled (bool): If it is set to True then only it will write the logs. Defaults to True.
            dataframe (spark.data.DataFrame): Spark Dataframe
            pipeline_path (str): Path to store the pipeline.
        """
        try:
            self.__logger = logger
            self.__dataframe = dataframe
            self.__stages = []
            self.__pipeline_path = pipeline_path
            self.__logger.log("Data_Preprocessing class started")
        except Exception:
            error = Data_Preprocessing_Exception("Error in module {0} class {1} method {2}".format(
                Data_Preprocessing.__module__.__str__, Data_Preprocessing.__class__.__name__, "__init__"), sys)
            raise error

    def encode_catagorical_features(self, features_list: list):
        """Used to encode the catagorical features just like one hot encoding

        Args:
            features_list (list): columns of the dataframe that you want to encode.
        """
        try:
            indexer = StringIndexer(
                inputCols=features_list, outputCols=[str(feature)+"_encoder" for feature in features_list])
            self.__stages.append(indexer)
            self.__logger.log("Done with the StringIndexer")
            onehot = OneHotEncoder(inputCols=indexer.getOutputCols(), outputCols=[
                str(feature)+"_encoderd" for feature in features_list])
            self.__stages.append(onehot)
            self.__logger.log("Done with the OneHotEncoder")
            self.__logger.log(
                "Successfully executed the encode_catagorical_features funtion")
        except Exception:
            error = Data_Preprocessing_Exception("Error in module {0} class {1} method {2}".format(
                Data_Preprocessing.__module__.__str__, Data_Preprocessing.__class__.__name__, self.encode_catagorical_features.__name__), sys)
            raise error

    def create_input_features(self, required_features):
        try:
            va = VectorAssembler(inputCols=required_features,
                                 outputCol="input_features")
            self.__stages.append(va)
            self.__logger.log("Done with the VectorAssembler")
            self.__logger.log(
                "Successfully executed the create_input_features funtion")
        except Exception:
            error = Data_Preprocessing_Exception("Error in module {0} class {1} method {2}".format(
                Data_Preprocessing.__module__.__str__, Data_Preprocessing.__class__.__name__, self.create_input_features.__name__), sys)
            raise error

    def splitting_the_dataframe(self, test_size=0.2, random_state=42):
        """Responsible fpr splitting the dataframe.

        Args:
            test_size (float, optional):size of the test data. Defaults to 0.2.
            random_state (float, optional):Random State. Defaults to 42.
        """
        try:
            self.__logger.log(
                f"test size is {test_size} random state is {random_state}")
            return self.__dataframe.randomSplit(
                weights=[1-test_size, test_size], seed=random_state)
        except Exception:
            error = Data_Preprocessing_Exception("Error in module {0} class {1} method {2}".format(
                Data_Preprocessing.__module__.__str__, Data_Preprocessing.__class__.__name__, self.splitting_the_dataframe.__name__), sys)
            raise error

    def update_dataframe_scheme(self, schema_definition):
        try:
            for col_name, dtype in schema_definition.items():
                self.__dataframe = self.__dataframe.withColumn(
                    col_name, self.__dataframe[col_name].cast(dtype))
            self.__logger.log(
                f"Sucessfully updated the dataframe schema")
            self.__logger.log(
                f"Sucessfully executed the update_dataframe_scheme funtion")
        except Exception:
            error = Data_Preprocessing_Exception("Error in module {0} class {1} method {2}".format(
                Data_Preprocessing.__module__.__str__, Data_Preprocessing.__class__.__name__, self.update_dataframe_scheme.__name__), sys)
            raise error

    def get_prepaired_data_set(self):
        try:
            schema_definition = {"age": IntegerType(),
                                 "sex": StringType(),
                                 "bmi": FloatType(),
                                 "children": IntegerType(),
                                 "smoker": StringType(),
                                 "expenses": FloatType()
                                 }
            self.update_dataframe_scheme(schema_definition=schema_definition)
            self.encode_catagorical_features(
                utility.read_params()['data']['catagorical_columns'])
            self.create_input_features(utility.read_params(
            )['data']['required_features_for_Vector_Assembler'])
            pipeline = Pipeline(stages=self.__stages)
            pipline_fitted_obj = pipeline.fit(self.__dataframe)
            self.__dataframe = pipline_fitted_obj.transform(self.__dataframe)
            utility.create_directory(self.__pipeline_path)
            pipline_fitted_obj.write().overwrite().save(self.__pipeline_path)
            self.__logger.log(
                f"Sucessfully save the pipeline path at {self.__pipeline_path}")
            return self.splitting_the_dataframe(test_size=float(utility.read_params()[
                'base']['test_size']), random_state=int(utility.read_params()[
                    'base']['random_state']))
        except Exception:
            error = Data_Preprocessing_Exception("Error in module {0} class {1} method {2}".format(
                Data_Preprocessing.__module__.__str__, Data_Preprocessing.__class__.__name__, self.get_prepaired_data_set.__name__), sys)
            raise error


class model_training:
    def __init__(self, logger, config):
        """Responsible for training of the model.

        Args:
            logger (logger_object): Object of the logger.
            config (str): path of the config file.
        """
        try:
            self.__logger = logger
            self.__config = utility.read_params(config)
            self.__ss = Spark_Manager().get_SparkSession()
            self.model_path = self.__config['artifacts']['model']['model_path']
            self.__logger.log(f"model path is loaded at {self.model_path}")
        except Exception:
            error = Data_Preprocessing_Exception("Error in module {0} class {1} method {2}".format(
                model_training.__module__.__str__, model_training.__class__.__name__, "__init__"), sys)
            raise error

    def get_dataframe(self):
        try:
            master_file_path = os.path.join(self.__config['artifacts']['Data_Directories']['training']['master_csv_path'],
                                            self.__config['artifacts']['Data_Directories']['training']['master_csv_filename'])
            df = self.__ss.read.csv(
                path=master_file_path, header=True, inferSchema=True)
            self.__logger.log(
                f"Sucessfully executed the get_dataframe funtion")
            return df
        except Exception:
            error = Data_Preprocessing_Exception("Error in module {0} class {1} method {2}".format(
                model_training.__module__.__str__, model_training.__class__.__name__, self.get_dataframe.__name__), sys)
            raise error

    def get_prepared_data(self):
        """Prepared the dataset from Data_Preprocessing class
        """
        try:
            pipelining_path = self.__config['artifacts']['Data_Directories']['training']['pipelining_path']
            dp = Data_Preprocessing(self.__logger,
                                    self.get_dataframe(), pipelining_path)
            self.__logger.log(
                f"Sucessfully executed the get_prepared_data funtion")
            return dp.get_prepaired_data_set()
        except Exception:
            error = Data_Preprocessing_Exception("Error in module {0} class {1} method {2}".format(
                model_training.__module__.__str__, model_training.__class__.__name__, self.get_prepared_data.__name__), sys)
            raise error

    def start_training(self):
        """Responsible for triggering the training process of the model
        """
        try:
            df_train, df_test = self.get_prepared_data()
            random_forest_regressor = RandomForestRegressor(
                featuresCol="input_features", labelCol="expenses")
            random_forest_model = random_forest_regressor.fit(df_train)
            train_prediction = random_forest_model.transform(df_train)
            testing_prediction = random_forest_model.transform(df_test)
            training_data = train_prediction.select(
                "expenses", "prediction").toPandas()
            testing_data = testing_prediction.select(
                "expenses", "prediction").toPandas()
            self.save_regression_metric_data(training_data['expenses'], training_data['prediction'],
                                             title="Training score")
            self.save_regression_metric_data(testing_data['expenses'], testing_data['prediction'],
                                             title="Testing score")

            self.save_model(model=random_forest_model,
                            model_name=self.__config['artifacts']['model']['model_name'])
            self.__ss.stop()
            self.__logger.log(
                f"Sucessfully done with the start_training funtion")
        except Exception:
            error = Data_Preprocessing_Exception("Error in module {0} class {1} method {2}".format(
                model_training.__module__.__str__, model_training.__class__.__name__, self.start_training.__name__), sys)
            raise error

    def save_model(self, model, model_name, intermediate_path=None):
        try:
            if intermediate_path is None:
                model_path = os.path.join(self.model_path)
            else:
                model_path = os.path.join(self.model_path, intermediate_path)
            utility.create_directory(model_path)
            model_full_path = os.path.join(model_path, f"{model_name}")
            model.write().overwrite().save(model_full_path)
            self.__logger.log(
                f"Sucessfully save the model to path {model_full_path}")
        except Exception:
            error = Data_Preprocessing_Exception("Error in module {0} class {1} method {2}".format(
                model_training.__module__.__str__, model_training.__class__.__name__, self.save_model.__name__), sys)
            raise error

    def save_regression_metric_data(self, y_true, y_pred, title):
        try:
            rmse = np.sqrt(mean_squared_error(y_true, y_pred))
            r_squared_score = r2_score(y_true, y_pred)
            msg = f"{title} R squared score: {r_squared_score:.3%}"
            print(msg)
            self.__logger.log(f"{msg}")
            msg = f"{title} Root mean squared error: {rmse:.3}"
            print(msg)
            self.__logger.log(f"{msg}")
            self.__logger.log(
                f"Sucessfully executed the funtion save_regression_metric_data")
        except Exception:
            error = Data_Preprocessing_Exception("Error in module {0} class {1} method {2}".format(
                model_training.__module__.__str__, model_training.__class__.__name__, self.save_regression_metric_data.__name__), sys)
            raise error


def validate_data_training(log_collection_name, is_log_enabled, config):
    """Responsible to execute the pipeline.

    Args:
        log_collection_name (str): Name of the collection in MongoDb database.
        config (str): Configuration file.
        is_log_enabled (bool): If it is set to True then only it will write the logs. Defaults to True.
    """
    logger = utility.get_log_object_for_training(
        collection_name=log_collection_name, is_log_enabled=is_log_enabled)
    logger.log("Validate main started....")
    mt = model_training(logger, config)
    logger.log("Model training is started....")
    mt.start_training()
    logger.log("Model training executed sucessfully....")


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
    validate_data_training(log_collection_name=parsed_args.log_collection_name,
                           config=parsed_args.config, is_log_enabled=parsed_args.is_log_enabled)
