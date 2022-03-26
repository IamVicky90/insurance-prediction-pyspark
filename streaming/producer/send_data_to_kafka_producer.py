import os
from kafka import KafkaProducer
import utility
import time
from insurance_exception.insurance_exception import InsuranceException as Kafka_Producer_Exception
import sys
class SendDataToKafkaProducer:
    def __init__(self,ss, config):
        """This class is responsible to produce or send data to the given KafkaProducer via topic and server id

        Args:
            ss(sparksession object): Spark Session.
            config (str): Path of the configuration file.
        """
        try:
            self.__ss = ss
            self.__config = utility.read_params(config)
            self.__topic = self.__config['kafka']['topic']
            self.__kafka_bootstrap_server = self.__config['kafka']['kafka_bootstrap_server']
            self.__kafka_producer = KafkaProducer(
                bootstrap_servers=self.__kafka_bootstrap_server, api_version=(0, 10, 2), value_serializer=lambda x: x.encode('utf-8'))
        except Exception:
            error = Kafka_Producer_Exception("Error in module {0} class {1} method {2}".format(
                SendDataToKafkaProducer.__module__.__str__, SendDataToKafkaProducer.__class__.__name__, "__init__"), sys)
            raise error
    def send_csv_data_to_kafka_producer(self,directory_path):
        """This will send csv data to kafka producer

        Args:
            directory_path (str): Path of the directory where batch files are present.
        """
        try:
            files=os.listdir(os.path.join(os.getcwd(),directory_path))
            for file in files:
                print(file)
                if file.endswith('.csv'):
                    df = self.__ss.read.csv(os.path.join(os.getcwd(), directory_path,file), header=True,inferSchema=True)
                    i=0
                    for row in df.rdd.toLocalIterator():
                        message = ",".join(map(str, list(row)))
                        print(message)
                        self.__kafka_producer.send(topic=self.__topic,value=message)
                        self.__kafka_producer.flush()
                        time.sleep(1)
        except Exception:
            error = Kafka_Producer_Exception("Error in module {0} class {1} method {2}".format(
                SendDataToKafkaProducer.__module__.__str__, SendDataToKafkaProducer.__class__.__name__, self.send_csv_data_to_kafka_producer.__name__), sys)
            raise error
                    
