from datetime import datetime
import random

class Config:
    """
    *
    * filename: config.py
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
    * Description: Class for configuration instance attributes
    """
    def __init__(self):
        self.training_data_path = 'data/training_data'
        self.training_database = 'training'
        self.prediction_data_path = 'data/prediction_data'
        self.prediction_database = 'prediction'

    def get_run_id(self):
        """
        * Method: get_run_id
        * Description: method to generate run id
        :return: None
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   none
        """
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H%M%S")
        return str(self.date) + "_" + str(self.current_time) + "_" + str(random.randint(100000000, 999999999))