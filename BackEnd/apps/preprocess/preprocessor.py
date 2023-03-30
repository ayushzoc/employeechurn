import pandas as pd
import numpy as np
import json
from sklearn.impute import KNNImputer
from apps.core.logger import Logger

class Preprocessor:
    """
    *
    * filename: preprocessor.py
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
    * Description: Class to preprocess training and predicting dataset
    """

    def __init__(self, run_id, data_path, mode):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'Preprocessor', mode)

    def get_data(self):
        """
        * Method: get_data
        * Description: method to read datafile
        :return: A Pandas DataFrame
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   none
        """
        try:
            # Reading the data file
            self.logger.info('Start of Reading Dataset...')
            self.data = pd.read_csv(self.data_path + "_validation/InputFile.csv")
            self.logger.info("End of Reading Dataset...")
            return self.data
        except Exception as e:
            self.logger.exception("Exception raised while Reading Dataset: %s" % e)
            raise Exception()

    def drop_columns(self, data, columns):
        """
        * Method: drop_columns
        * Description: method to drop columns
        :return: A pandas DataFrame after removing the specified column
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   data, columns
        """
        try:
            self.logger.info("Start of Dropping Columns...")
            self.useful_data = self.data.drop(labels = self.columns, axis = 1) # Drop the labels specified in the columns
            self.logger.info("End of Dropping COlumns...")
            return self.useful_data
        except Exception as e:
            self.logger.exception("Exception raised while Dropping Columns: %s" % e)
            raise Exception()

    def is_null_present(self, data):
        """
        * Method: is_null_present
        * Description: method to check null values
        :return: Returns Boolean Value. True if Null values are present in the DataFrame, False if not present
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   data
        """
        self.null_present = False
        try:
            self.logger.info("Start of Finding Missing Values...")
            self.null_counts = data.isna().sum() # Check for the count of null values per column
            for i in self.null_counts:
                if i > 0:
                    self.null_present = True
                    break
            if (self.null_present): # Write the logs to see which columns have null values
                dataframe_with_null = pd.DataFrame()
                dataframe_with_null['columns'] = data.columns
                dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
                dataframe_with_null.to_csv(self.data_path + "_validation/" + "null_values.csv") # Store the null column information to file
            self.logger.info("End of Finding Missing Values...")
            return self.null_present
        except Exception as e:
            self.logger.exception("Exception raised while Finding Missing values: %s" % e)
            raise Exception()

    def impute_missing_values(self, data):
        """
        * Method: impute_missing_values
        * Description: method to impute missing values
        :return: none
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   data
        """
        self.data = data
        try:
            self.logger.info("Start of Imputing Missing Values...")
            imputer = KNNImputer(n_neighbors = 3, weights = 'uniform', missing_values = np.nan)
            self.new_array = imputer.fit_transform(self.data)
            # Conver the nd-array returned in the step above to a DataFrame
            self.new_data = pd.DataFrame(data = self.new_array, columns = self.data.columns)
            self.logger.info("End of Imputing Missing Values...")
            return self.new_data
        except Exception as e:
            self.logger.exception("Exception raised while Imputing Missing Values: " + str(e))
            raise Exception()

    def feature_encoding(self, data):
        """
        * Method: feature_encoding
        * Description: method to encode features of columns
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
            self.logger.info("Start of Feature Encoding")
            self.new_data = data.select_dtypes(include = ['object'].copy())
            # Using dummy encoding to encode the categorical columns to numerical ones
            for col in self.new_data.columns:
                self.new_data = pd.get_dummies(self.new_data, columns = [col], prefix = [col], drop_first = True)
            self.logger.info("End of Feature Encoding")
            return self.new_data
        except Exception as e:
            self.logger.exception("Exception raised while Feature Encoding: " + str(e))
            raise Exception()

    def split_features_label(self, data, label_name):
        """
        * Method: split_features_label
        * Description: method to split features and label
        :return: none
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   data, label_name
        """
        self.data = data
        try:
            self.logger.info("Start of splitting features and label...")
            self.X = self.data.drop(labels = label_name, axis = 1) # Drop the columns specified and separate the feature columns
            self.y = self.data[label_name]
            self.logger.info("End of splitting features and label...")
            return self.X, self.y
        except Exception as e:
            self.logger.exception("Exception raised while Splitting Features and Label: " + str(e))
            raise Exception()

    def final_predictset(self, data):
        """
        * Method: final_predict_set
        * Description: method to build final predict set by adding additional encoded column with value as 0
        :return: column_names, Number of columns
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   data
        """
        try:
            self.logger.info("Start of building final predictset...")
            with open('apps/database/columns.json', 'r') as f:
                data_columns = json.load(f)['data_columns']
                f.close()
            df = pd.DataFrame(data = None, columns = data_columns)
            df_new = pd.concat([df, data], ignore_index = True, sort = False)
            data_new = df_new.fillna(0)
            self.logger.info("End of building final predictset...")
            return data_new
        except ValueError:
            self.logger.exception("ValueError raised while building final predictset")
            raise ValueError
        except KeyError:
            self.logger.exception("KeyError raised while building final predictset")
            raise KeyError
        except Exception as e:
            self.logger.exception("Exception raised while building final predictset: " + str(e))
            raise e

    def preprocess_trainset(self):
        """
        * Method: preprocess_trainset
        * Description: method to preprocess training set
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
            self.logger.info("Start of preprocessing...")
            # Get data into pandas data frame
            data = self.get_data()
            # Drop unwanted columns
            data = self.drop_columns(data, ['empid'])
            # Handle Label encoding
            cat_df = self.feature_encoding(data)
            data = pd.concat([data, cat_df], axis = 1)
            # Drop categorical column
            data = self.drop_columns(data, ['salary'])
            # Check if missing values are present in the data set
            is_null_present = self.is_null_present(data)
            # If missing values are present, replace them appropriately
            if (is_null_present):
                data = self.impute_missing_values(data) # Missing values imputation
            # Create separate features and labels
            self.X, self.y = self.split_features_label(data, label_name = 'left')
            self.logger.info("End of preprocessing...")
            return self.X, self.y
        except Exception:
            self.logger.exception('Unsuccessful End of Preprocessing...')
            raise Exception

    def preprocess_predictset(self):
        """
        * Method: preprocess_predictset
        * Description: method to preprocess prediction set
        :return: A pandas DataFrame after removing the specified column
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   none
        """
        try:
            self.logger.info("Start of Preorcessing Prediction set...")
            # Get data into pandas data frame
            data = self.get_data()
            # Drop unwanted columns
            # data = self.drop_columns(data, ['empid'])
            # handle label encoding
            cat_df = self.feature_encoding(data)
            data = pd.concat([data, cat_df], axis = 1)
            # drop categorical column
            data = self.drop_columns(data, ['salary'])
            # Check if missing values are present in the data set
            is_null_present = self.is_null_present(data)
            # If missing values are there, replace them appropriately.
            if (is_null_present):
                data = self.impute_missing_values(data) # Missing value imputation

            data = self.final_predictset(data)
            self.logger.info("End of Preprocessing Prediction set...")
            return data
        except Exception:
            self.logger.exception("Unsuccessful End of Preprocessing...")
            raise Exception

    def preprocess_predict(self, data):
        """
        * Method: preprocess_predict
        * Description: method to preprocess prediction set
        :return: A pandas DataFrame after removing the specified column
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   none
        """
        try:
            self.logger.info("Start of Preprocessing...")
            cat_df = self.feature_encoding(data)
            data = pd.concat([data, cat_df], axis = 1)
            # Drop categorical column
            data = self.drop_columns(data, ['salary'])
            # Check if missing values are present in the data set
            is_null_present = self.is_null_present(data)
            # If missing values are present, replace them appropriately
            if (is_null_present):
                data = self.impute_missing_values(data) # Missing values imputation

            data = self.final_predictset(data)
            self.logger.info("End of Preprocessing...")
            return data
        except Exception:
            self.logger.exception("Unsuccessful End of Preprocessing")
            raise Exception
