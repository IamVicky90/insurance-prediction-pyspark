import utility
import os
from spark_manager.spark_manager import Spark_Manager
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.types import IntegerType, StringType, FloatType
from pyspark.ml.regression import RandomForestRegressor
import numpy as np
from sklearn.metrics import r2_score, mean_squared_error
log_collection_name = "Training_model"
class Data_Preprocessing:
    def __init__(self,logger,is_log_enabled: bool,dataframe,pipeline_path:str):
        """Responsible for Data_Preprocessing.

        Args:
            logger (logger_object): Object of the logger.
            is_log_enabled (bool): If it is set to True then only it will write the logs. Defaults to True.
            dataframe (spark.data.DataFrame): Spark Dataframe
            pipeline_path (str): Path to store the pipeline.
        """
        self.__logger =logger
        self.__is_log_enabled = is_log_enabled
        self.__dataframe = dataframe
        self.__stages = []
        self.__pipeline_path = pipeline_path
    def encode_catagorical_features(self,features_list:list):
        """Used to encode the catagorical features just like one hot encoding

        Args:
            features_list (list): columns of the dataframe that you want to encode.
        """
        indexer = StringIndexer(
            inputCols=features_list, outputCols=[str(feature)+"_encoder" for feature in features_list])
        self.__stages.append(indexer)
        onehot = OneHotEncoder(inputCols=indexer.getOutputCols(), outputCols=[
                                       str(feature)+"_encoderd" for feature in features_list])
        self.__stages.append(onehot)
    def create_input_features(self,required_features):
        va = VectorAssembler(inputCols=required_features,
                             outputCol="input_features")
        self.__stages.append(va)

    def splitting_the_dataframe(self, test_size=0.2, random_state=42):
        """Responsible fpr splitting the dataframe.

        Args:
            test_size (float, optional):size of the test data. Defaults to 0.2.
            random_state (float, optional):Random State. Defaults to 42.
        """
        return self.__dataframe.randomSplit(
            weights=[1-test_size, test_size], seed=random_state)

    def update_dataframe_scheme(self, schema_definition):
        print(self.__dataframe.printSchema())
        for col_name, dtype in schema_definition.items():
            self.__dataframe = self.__dataframe.withColumn(
                col_name, self.__dataframe[col_name].cast(dtype))
        print(self.__dataframe.printSchema())
    def get_prepaired_data_set(self):
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
        pipeline=Pipeline(stages=self.__stages)
        pipline_fitted_obj=pipeline.fit(self.__dataframe)
        self.__dataframe=pipline_fitted_obj.transform(self.__dataframe)
        utility.create_directory(self.__pipeline_path)
        pipline_fitted_obj.write().overwrite().save(self.__pipeline_path)
        return self.splitting_the_dataframe(test_size=float(utility.read_params()[
            'base']['test_size']), random_state=int(utility.read_params()[
                'base']['random_state']))
class model_training:
    def __init__(self,logger,is_log_enabled,config):
        """Responsible for training of the model.

        Args:
            logger (logger_object): Object of the logger.
            is_log_enabled (bool): If it is set to True then only it will write the logs. Defaults to True.
            config (str): path of the config file.
        """
        self.__logger = logger
        self.__config = utility.read_params(config)
        self.__is_log_enabled=is_log_enabled
        self.__ss = Spark_Manager().get_SparkSession()
        self.model_path=self.__config['artifacts']['model']['model_path']
    def get_dataframe(self):
        master_file_path=os.path.join(self.__config['artifacts']['Data_Directories']['training']['master_csv_path'],
        self.__config['artifacts']['Data_Directories']['training']['master_csv_filename'])
        df = self.__ss.read.csv(
            path=master_file_path, header=True, inferSchema=True)
        return df
    def get_prepared_data(self):
        """Prepared the dataset from Data_Preprocessing class
        """
        pipelining_path=self.__config['artifacts']['Data_Directories']['training']['pipelining_path']
        dp = Data_Preprocessing(self.__logger, True,
                                self.get_dataframe(), pipelining_path)
        return dp.get_prepaired_data_set()
    def start_training(self):
        """Responsible for triggering the training process of the model
        """
        df_train,df_test=self.get_prepared_data()
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

    def save_model(self, model, model_name, intermediate_path=None):

        if intermediate_path is None:
            model_path = os.path.join(self.model_path)
        else:
            model_path = os.path.join(self.model_path, intermediate_path)
        utility.create_directory(model_path )
        model_full_path = os.path.join(model_path, f"{model_name}")
        model.write().overwrite().save(model_full_path)

    def save_regression_metric_data(self, y_true, y_pred, title):
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r_squared_score = r2_score(y_true, y_pred)
        msg = f"{title} R squared score: {r_squared_score:.3%}"
        print(msg)
        msg = f"{title} Root mean squared error: {rmse:.3}"
        print(msg)
    

if __name__ =="__main__":
    mt = model_training('', True, 'config/params.yaml')
    mt.start_training()
