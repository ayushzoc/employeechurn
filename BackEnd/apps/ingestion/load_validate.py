import json
from os import listdir
import shutil
import pandas as pd
from datetime import datetime
import os
from apps.database.database_operation import DatabaseOperation
from apps.core.logger import Logger

class LoadValidate:
    """
    *
    * filename: load_validate.py
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
    * Description: Class to load, validate, and transform the data
    """

    def __init__(self, run_id, data_path, mode):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'LoadValidate', mode)
        self.dbOperation = DatabaseOperation(self.run_id, self.data_path, mode)

    def values_from_schema(self, schema_file):
        """
        * Method: database_connection
        * Description: method to read values from schema
        :return: none
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   schema_file
        """
        try:
            self.logger.info("Start of Reading Values from Schema...")
            with open('apps/database/' + schema_file + ".json", "r") as f:
                dic = json.load(f)
                f.close()
            column_names = dic['ColName']
            number_of_columns = dic['NumberofColumns']
            self.logger.info("End of Reading Values from Schema...")
        except ValueError:
            self.logger.exception("ValueError raised while Reading Values from Schema")
            raise ValueError
        except KeyError:
            self.logger.exception("KeyError raised while Reading Values from Schema")
            raise KeyError
        except Exception as e:
            self.logger.exception("Exception raised while Reading Values from Schema: %s" % e)
            raise e
        return column_names, number_of_columns

    def validate_column_length(self, number_of_columns):
        """
        * Method: validate_column_length
        * Description: method to validate the number of columns in the csv files
        :return: none
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   number_of_columns
        """
        try:
            self.logger.info("Start of Validating Column Length...")
            for file in listdir(self.data_path):
                csv = pd.read_csv(self.data_path + "/" + file)
                if csv.shape[1] == number_of_columns:
                    pass
                else:
                    shutil.move(self.data_path + "/" + file, self.data_path + "_rejects")
                    self.logger.info("Invalid Column Lengths: %s" % file)
            self.logger.info("End of Validation Column Length...")
        except OSError:
            self.logger.exception("OSError raised while Validating Column Length")
            raise OSError
        except Exception as e:
            self.logger.exception("Exception raised while validating Column Length: %s" % e)
            raise e

    def validate_missing_values(self):
        """
        * Method: validate_missing_values
        * Description: method to validate if any column in the csv file has all values missing,
        *               If all the values are missing, the file is not suitable for processing. It will be moved to bad file
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
            self.logger.info("Start of Validating Missing Values...")
            for file in listdir(self.data_path):
                csv = pd.read_csv(self.data_path + "/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count += 1
                        shutil.move(self.data_path + "/" + file, self.data_path + "_rejects")
                        self.logger.info("All Missing Values in Column: %s" % file)
                        break
            self.logger.info("End of Validating Missing Values...")
        except OSError:
            self.logger.exception("OSError raised while Validating Missing Values")
            raise OSError
        except Exception as e:
            self.logger.exception("Exception raised while Validing Missing Values: %s" % e)
            raise e

    def replace_missing_values(self):
        """
        * Method: replace_missing_values
        * Description: method to replace missing values with "NULL"
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
            self.logger.info("Start of Replacing Missing Values with NULL...")
            only_files = [f for f in listdir(self.data_path)]
            for file in only_files:
                csv = pd.read_csv(self.data_path + "/" + file)
                csv.fillna("NULL", inplace = True)
                csv.to_csv(self.data_path + "/" + file, index = None, header = True)
                self.logger.info("%s: File Transformed Successfully!!" % file)
            self.logger.info("End of Replacing Missing Values with NULL...")
        except Exception as e:
            self.logger.exception("Exception raised while Replacing Missing Values with NULL: %s", % e)
            raise e

    def archive_old_files(self):
        """
        * Method: archive_old_files
        * Description: method to archive rejected files
        :return: none
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   none
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            self.logger.info("Start of Archiving Old Rejected Files...")
            source = self.data_path + "_rejects/"
            if os.path.isdir(source):
                path = self.data_path + "_archive"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = path + "/reject_" + str(date) + "_" + str(time)
                files = os.listdir(source)
                for f in files:
                    if not os.path.isdir(dest):
                        os.makedirs(dest)
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
            self.logger.info("End of Archiving Old Rejected Files...")

            self.logger.info("Start of Archiving Old Validated Files...")
            source = self.data_path + "_validation/"
            if os.path.isdir(source):
                path = self.data_path + "_archive"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = path + "/validation_" + str(date) + "_" + str(time)
                files = os.listdir(source)
                for f in files:
                    if not os.path.isdir(dest):
                        os.makedirs(dest)
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
            self.logger.info("End of Archiving Old Validated Files...")

            self.logger.info("Start of Archiving Old Processed Files...")
            source = self.data_path + "_processed/"
            if os.path.isdir(source):
                path = self.data_path + "_archive"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = path + "/processed_" + str(date) + "_" + str(time)
                files = os.listdir(source)
                for f in files:
                    if not os.path.isdir(dest):
                        os.makedirs(dest)
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
            self.logger.info("End of Archiving Old Processed Files...")

            self.logger.info("Start of Archiving Old Result Files...")
            source = self.data_path + "results/"
            if os.path.isdir(source):
                path = self.data_path + "_archive"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = path + "/results_" + str(date) + "_" + str(time)
                files = os.listdir(source)
                for f in files:
                    if not os.path.isdir(dest):
                        os.makedirs(dest)
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
            self.logger.info("End of Archiving Old Result Files...")

        except Exception as e:
            self.logger.exception("Exception raised while Archiving Old Rejected Files: %s" % e)
            raise e

    def move_processed_files(self):
        """
        * Method: move_processed_files
        * Description: method to move processed file
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
            self.logger.info("Start of Moving Processed Files...")
            for file in listdir(self.data_path):
                shutil.move(self.data_path + "/" + file, self.data_path + "_processed")
                self.logger.info("Moved the already processed file %s" % file)
            self.logger.info("End of Moving Processed Files...")
        except Exception as e:
            self.logger.exception("Exception raised while Moving Processed Files: %s" % e)
            raise e

    def validate_trainset(self):
        """
        * Method: validate_trainset
        * Description: method to validate the data
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
            self.logger.info("Start of Data Load, Validation and Transformation")
            # Archive Old Files
            self.archive_old_files()
            # Extracting Values from Training Schema
            column_names, number_of_columns = self.values_from_schema('schema_train')
            # Validating Column Length in the File
            self.validate_column_length(number_of_columns)
            # Validating if any column has all values missing
            self.validate_missing_values()
            # Replacing Blanks in the csv file with "NULL" values
            self.replace_missing_values()
            # Create Database with given name, if present open the connection. Create table with columns given in schema
            self.dbOperation.create_table('training', 'training_raw_data_t', column_names)
            # Insert CSV files in the table
            self.dbOperation.insert_data('training', 'training_raw_data_t')
            # Export Data in table to csv file
            self.dbOperation.export_csv('training', 'training_raw_data_t')
            # Move Processed Files
            self.move_processed_files()
            self.logger.info("End of Data Load, Validation and Transformation")
        except Exception:
            self.logger.exception("Unsuccessful End of Data Load, Validation, and Transformation")
            raise Exception

    def validate_predictset(self):
        """
        * Method: validate_predictset
        * Description: method to validate the predict data
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
            self.logger.info("Start of Data Load, Validation, and Transformation")
            # Archive old rejected files
            self.archive_old_files()
            # Extracting values from schema
            column_names, number_of_columns = self.values_from_schema('schema_predict')
            # Validating Column Length in the file
            self.validate_column_length(number_of_columns)
            # Validating if any column has all values missing
            self.validate_missing_values()
            # Replacing blanks in the csv file with "NULL" values
            self.replace_missing_values()
            # Create database with given name. If present open the connection. Create table with columns given in schema
            self.dbOperation.create_table('prediction', 'prediction_raw_data_t', column_names)
            # Insert CSV files in the table
            self.dbOperation.insert_data('prediction', 'prediction_raw_data_t')
            # Export data in table to csv file
            self.move_processed_files()
            self.logger.info("End of Data Load, Validation, and Transformation")
        except Exception:
            self.logger.exception("Unsuccessful End of Data Load, Validation, and Transformation")
            raise Exception
