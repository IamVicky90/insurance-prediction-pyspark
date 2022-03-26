from streaming.consumer.consume_data_from_kafka import Consume_Data_From_Kafka
from spark_manager.spark_manager import Spark_Manager
import os
import utility
import sys
try:
    sd = Consume_Data_From_Kafka(
        Spark_Manager().get_SparkSession(), os.path.join("config", "params.yaml"))
    sd.receive_data_from_kafka()

except Exception:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    raise Exception("error type: "+str(exc_type) + " filename: " +
                    str(fname)+" line no: " + str(exc_tb.tb_lineno))
