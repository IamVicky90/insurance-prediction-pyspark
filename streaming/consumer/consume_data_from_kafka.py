import utility
from pyspark.sql.functions import *
from pyspark.ml import PipelineModel
from pyspark.ml.regression import RandomForestRegressionModel
from mongo_db.mongo_db_atlas import Mongo_operations
import sys
from insurance_exception.insurance_exception import InsuranceException as Kafka_Consume_Exception
class Consume_Data_From_Kafka:
    def __init__(self,ss,config):
        """Responsible to consume the data from kafka cluster.

        Args:
            ss (SPark Session): Spark Session.
            config (str): Configuration path
        """
        try:
            self.__ss=ss
            self.__config = utility.read_params(config)
            self.__topic = self.__config['kafka']['topic']
            self.__kafka_bootstrap_server = self.__config['kafka']['kafka_bootstrap_server']
            self.__db=Mongo_operations()
            self.__collection_name=self.__config["streaming_data"]["collection_name"]
            self.__processingTime = self.__config["streaming_data"]["processingTime"]
            self.__database_name = self.__config["database_for_data"]["database_name"]
        except Exception:
            error = Kafka_Consume_Exception("Error in module {0} class {1} method {2}".format(
                Consume_Data_From_Kafka.__module__, Consume_Data_From_Kafka.__class__.__name__, "__init__"), sys)
            raise error
    def receive_data_from_kafka(self):
        """Receive Data From Kafka Cluster
        """
        try:
            dataframe = self.__ss \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.__kafka_bootstrap_server) \
                .option("subscribe", self.__topic) \
                .option("startingOffsets", "latest") \
                .load()
            
            dataframe_1 = dataframe.selectExpr(
                "CAST(value as STRING) ", "timestamp")
            dataframe_2 = dataframe_1.select(
                from_csv(col("value"), "age INT,sex STRING,bmi DOUBLE,children INT,smoker STRING,region STRING").alias("records"), "timestamp")
            dataframe_3 = dataframe_2.select("records.*", "timestamp")
            
            querry = dataframe_3.writeStream.trigger(
                processingTime=self.__processingTime).foreachBatch(self.predict_data).start()
            querry.awaitTermination()
        except Exception:
            error = Kafka_Consume_Exception("Error in module {0} class {1} method {2}".format(
                Consume_Data_From_Kafka.__module__, Consume_Data_From_Kafka.__class__.__name__, self.receive_data_from_kafka.__name__), sys)
            raise error
    def predict_data(self,dataframe,epoch_id):
        """Responsible to predict the data

        Args:
            dataframe (Spark Dataframe): Pyspark Dataframe
            epoch_id (int): Epoch of the current prediction.
        """
        try:
            pipelining = PipelineModel.load('artifacts/pipelining')
            model = RandomForestRegressionModel.load(
                'artifacts/training_data/model/random_forest_regressor')
            process_data = pipelining.transform(dataframe)
            predict_dataframe = model.transform(process_data)
            df=predict_dataframe.select('age', 'sex', 'bmi',
                'children', 'smoker', 'region','prediction')
            print(df.show())
            if df.count()>0:
                self.__db.insert_many_records(records=df.toPandas().T.to_dict().values(
                ), dbname=self.__database_name,collection_name= self.__collection_name)
        except Exception:
            error = Kafka_Consume_Exception("Error in module {0} class {1} method {2}".format(
                Consume_Data_From_Kafka.__module__, Consume_Data_From_Kafka.__class__.__name__, self.predict_data.__name__), sys)
            raise error
