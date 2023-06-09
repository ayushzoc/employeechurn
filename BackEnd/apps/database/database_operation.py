import sqlite3
import csv
from os import listdir
import shutil
import os

from apps.core.logger import Logger

class DatabaseOperation:
    """
    *
    * filename: database_operation.py
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
    * Description: Class to handle database operations
    """

    def __init__(self, run_id, data_path, mode):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'DatabaseOperation', mode)

    def database_connection(self, database_name):
        """
        * Method: database_connection
        * Description: method to build database connection
        :return: none
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   database_name
        """
        try:
            conn = sqlite3.connect('apps/database/' + database_name + '.db')
            self.logger.info('Opened %s database successfully' % database_name)
        except ConnectionError:
            self.logger.info('Error while connection to database: %s' %ConnectionError)
            raise ConnectionError
        return conn

    def create_table(self, database_name, table_name, column_names):
        """
        * Method: create_table
        * Description: method to build database connection
        :return: none
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   database_name
        *   table_name
        *   column_names
        """

        try:
            self.logger.info('Start of Creating Table...')
            conn = self.database_connection(database_name)

            if (database_name == "prediction"):
                conn.execute("DROP TABLE IF EXISTS '" + table_name + "';")

            c = conn.cursor()
            c.execute("SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = '"+ table_name +"'")
            if c.fetchone()[0] == 1:
                conn.close()
                self.logger.info('Tables created successfully')
                self.logger.info("Closed %s database successfully" % database_name)
            else:
                for key in column_names.keys():
                    type = column_names[key]

                    # In try block we check if the table exists. If yes, then add columns to the table
                    # Else in catch block, we will create the table --training_raw_data_t
                    try:
                        conn.execute("ALTER TABLE " + table_name + " ADD COLUMN {column_name} {dataType})".format(column_name = key, dataType = type))
                        self.logger.info("ALTER TABLE " + table_name + " ADD COLUMN")
                    except:
                        conn.execute("CREATE TABLE " + table_name + " ({column_name} {dataType})".format(column_name = key, dataType = type))
                        self.logger.info("CREATE TABLE " + table_name + " column_name")
                conn.close()
            self.logger.info('End of Creating Table...')
        except Exception as e:
            self.logger.exception("Exception raised while Creating Table: %s" % e)
            raise e

    def insert_data(self, database_name, table_name):
        """
        * Method: insert_data
        * Description: method to insert data into table
        :return: none
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   database_name
        *   table_name
        """
        conn = self.database_connection(database_name)
        good_data_path = self.data_path
        bad_data_path = self.data_path + "_rejects"
        only_files = [f for f in listdir(good_data_path)]
        self.logger.info('Start of Inserting Data into Table...')
        for file in only_files:
            try:
                with open(good_data_path + '/' + file, "r") as f:
                    next(f)
                    reader = csv.reader(f, delimiter = ',')
                    for line in enumerate(reader):
                        to_db = ''
                        for list_ in (line[1]):
                            try:
                                to_db = to_db + ",'" + list_ + "'"
                            except Exception as e:
                                raise e
                        to_db = to_db.lstrip(',')
                        conn.execute("INSERT INTO " + table_name + " values ({values})".format(values = (to_db)))
                        conn.commit()
            except Exception as e:
                conn.rollback()
                self.logger.exception('Exception raised while Inserting Data into Table: %s' % e)
                shutil.move(good_data_path + '/' + file, bad_data_path)
                conn.close()
        conn.close()
        self.logger.info('End of Inserting Data into Table...')

    def export_csv(self, database_name, table_name):
        """
        * Method: export_csv
        * Description: method to select data from table in export into csv
        :return: none
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       03/29/2023  1.0         Initial Creation
        *
        * Parameters:
        *   database_name
        *   table_name
        """
        self.file_from_db = self.data_path + str('_validation/')
        self.file_name = 'InputFile.csv'
        try:
            self.logger.info("Start of Exporting Data into CSV...")
            conn = self.database_connection(database_name)
            sqlSelect = "SELECT * FROM " + table_name + ""
            cursor = conn.cursor()
            cursor.execute(sqlSelect)
            results = cursor.fetchall()
            # Get the headers of the csv file
            headers = [i[0] for i in cursor.description]
            # Make the CSV output directory
            if not os.path.isdir(self.file_from_db):
                os.makedirs(self.file_from_db)
            # Open CSV file for writing
            csv_file = csv.writer(open(self.file_from_db + self.file_name, 'w', newline = ''), delimiter = ',', lineterminator = '\r\n', quoting = csv.QUOTE_ALL, escapechar = '\\')
            # Add the headers and data to the CSV file
            csv_file.writerow(headers)
            csv_file.writerows(results)
            self.logger.info("End of Exporting Data into CSV...")
        except Exception as e:
            self.logger.expcetion("Exception raised while Exporting Data into CSV: %s" % e)