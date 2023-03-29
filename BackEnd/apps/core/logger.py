from datetime import datetime
import logging


class Logger:
    """
    *
    * filename: logger.py
    * version: 1.0
    * author: Ray Joshi
    * Creation date: 03/29/2023
    *
    * Change History:
    *
    * who       when        version     change (include bug = if apply)
    * -----     -------     -------     -------------------------------
    * Ray       03/29/2023  1.0         Initial Creation
    *
    *
    * Description: Class to generate logs
    """

    def __init__(self, run_id, log_module, log_file_name):
        self.logger = logging.getLogger(str(log_module) + '_' + str(run_id))
        self.logger.setLevel(logging.DEBUG)
        if log_file_name == "training":
            file_handler = logging.FileHandler('logs/training_logs/train_log_' + str(run_id) + '.log')
        else:
            file_handler = logging.FileHandler('logs/prediction_logs/prediction_log_' + str(run_id) + '.log')

        formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def info(self, message):
        self.logger.info(message)