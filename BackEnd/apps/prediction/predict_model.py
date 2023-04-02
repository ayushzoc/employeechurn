import pandas as pd
from apps.core.logger import Logger
from apps.ingestion.load_validate import LoadValidate
from apps.preprocess.preprocessor import Preprocessor
from apps.core.file_operation import FileOperation

class PredictionModel:
    """
    *
    * filename: predict_model.py
    * version: 1.0
    * author: Ray Joshi
    * Creation date: 03/31/2023
    *
    * Change History:
    *
    * who       when        version     change (include bug = if apply)
    * -----     -------     -------     -------------------------------
    * Ray       03/29/2023  1.0         Initial Creation
    *
    *
    * Description: Class to predict the result
    """
    def __init__(self, run_id, data_path):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'PredictionModel', 'prediction')
        self.loadValidate = LoadValidate(self.run_id, self.data_path, 'prediction')
        self.preProcess = Preprocessor(self.run_id, self.data_path, 'prediction')
        self.fileOperation = FileOperation(self.run_id, self.data_path, 'prediction')

    def batch_predict_from_model(self):
        """
        * Method: batch_predict_from_model
        * Description: method to prediction the results
        :return: none
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   none
        """
        try:
            self.logger.info("Start of Prediction")
            self.logger.info('run_id: ' + str(self.run_id))
            # Validation and Transformation
            self.loadValidate.validate_predictset()
            # Preprocessing activities
            self.X = self.preProcess.preprocess_predictset()
            # Load Model
            kmeans = self.fileOperation.load_model('KMeans')
            # Cluster selection
            clusters = kmeans.predict(self.X.drop(['empid'], axis = 1))
            self.X['clusters'] = clusters
            clusters = self.X['clusters'].unique()
            y_predicted = []

            for i in clusters:
                self.logger.info("Clusters loop started")
                cluster_data = self.X[self.X['clusters'] == i]
                cluster_data_new = cluster_data.drop(['empid', 'clusters'], axis = 1)
                model_name = self.fileOperation.correct_model(i)
                model = self.fileOperation.load_model(model_name)
                y_predicted = model.predict(cluster_data_new)
                result = pd.DataFrame({"Empid": cluster_data['empid'], "Prediction": y_predicted})
                result.to_csv(self.data_path + "_results/" + "Predictions.csv", header = True, mode = "a+", index = False)
            self.logger.info("End of Prediction")
        except Exception:
            self.logger.exception("Unsuccessful End of Prediction")
            raise Exception

    def single_predict_from_model(self, data):
        """
        * Method: single_predict_from_model
        * Description: method to predict the results
        :return: none
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   data
        """
        try:
            self.logger.info("Start of Prediction")
            self.logger.info("run_id: " + str(self.run_id))
            # Preprocessing activities
            self.X = self.preProcess.preprocess_predict(data)
            # Load Model
            kmeans = self.fileOperation.load_model('KMeans')
            # Cluster selection
            clusters = kmeans.predict(self.X.drop(['empid'], axis = 1))
            self.X['clusters'] = clusters
            clusters = self.X['clusters'].unique()
            y_predicted = []
            for i in clusters:
                self.logger.info("Clusters loop started")
                cluster_data = self.X[self.X['clusters'] == i]
                cluster_data_new = cluster_data.drop(['empid', 'clusters'], axis = 1)
                model_name = self.fileOperation.correct_model(i)
                model = self.fileOperation.load_model(model_name)
                self.logger.info("Shape of Data " + str(cluster_data_new.shape))
                self.logger.info("Shape of Data " + str(cluster_data_new.info()))
                y_predicted = model.predict(cluster_data_new)
                self.logger.info("Output: " + str(y_predicted))
                self.logger.info("End of Prediction")
                return int(y_predicted[0])
        except Exception:
            self.logger.exception("Unsuccessful End of Prediction")
            raise Exception

























