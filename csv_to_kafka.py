from streaming.producer.send_data_to_kafka_producer import SendDataToKafkaProducer
from spark_manager.spark_manager import Spark_Manager
import os
import utility
import sys
try:
    utility.op()
    sd = SendDataToKafkaProducer(
        Spark_Manager().get_SparkSession(), os.path.join("config","params.yaml"))
    sd.send_csv_data_to_kafka_producer(utility.read_params()["batch_file"]["training_batch_files_path"])

except Exception:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    raise Exception("error type: "+str(exc_type)+ " filename: "+str(fname)+" line no: " +str(exc_tb.tb_lineno))
