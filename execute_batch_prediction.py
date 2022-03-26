import argparse
import ast
import sys
import os
import utility
from prediction.data_prediction_stage_04 import validate_data_prediction
log_collection_name="Batch Prediction"
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
