import utility
import os
from spark_manager.spark_manager import Spark_Manager
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.types import IntegerType, StringType, FloatType
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
        df_train, df_test = self.__dataframe.randomSplit(
            weights=[1-test_size, test_size], seed=random_state)

    def update_dataframe_scheme(self, schema_definition):
        print(self.__dataframe.printSchema())
        for col_name, dtype in schema_definition.items():
            self.__dataframe = self.__dataframe.withColumn(
                col_name, self.__dataframe[col_name].cast(dtype))
        print('.......................................')
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
        

if __name__ =="__main__":
    ss = Spark_Manager().get_SparkSession()
    df = ss.read.csv(
       path= "artifacts/training_data/master_csv/Master.csv", header=True, inferSchema=True)
    dp=Data_Preprocessing('',True,df,'pipelining')
    dp.get_prepaired_data_set()
