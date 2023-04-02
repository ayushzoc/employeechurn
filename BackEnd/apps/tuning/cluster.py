from apps.core.logger import Logger
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from sklearn.model_selection import train_test_split
from apps.core.file_operation import FileOperation
from apps.tuning.model_tuner import ModelTuner
from apps.ingestion.load_validate import LoadValidate
from apps.preprocess.preprocessor import Preprocessor

class KMeansCluster:
    """
   *
   * filename: cluster.py
   * version: 1.0
   * author: Ray Joshi
   * Creation date: 04/02/2023
   *
   * Change History:
   *
   * who       when        version     change (include bug = if apply)
   * -----     -------     -------     -------------------------------
   * Ray       03/29/2023  1.0         Initial Creation
   *
   *
   * Description: Class to cluster the dataset
   """
    def __init__(self, run_id, data_path):
        self.run_id = run_id
        self.data_path = data_path
        self.logger = Logger(self.run_id, 'KMeansCluster', 'training')
        self.fileOperation = FileOperation(self.run_id, self.data_path, 'training')

    def elbow_plot(self, data):
        """
        * Method: elbow_plot
        * Description: method to save the plot to decide the optimum number of clusters to the file.
        :return: A picture saved to directory
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       04/02/2023  1.0         Initial Creation
        *
        * Parameters:
        *   data
        """
        wcss = [] # Initializing an empty list -- within cluster sum of errors
        try:
            self.logger.info("Start of Elbow Plotting...")
            for i in range(1, 11):
                kmeans = KMeans(n_clusters = i, init = 'k-means++', random_state = 0) # Initializing the KMeans object
                kmeans.fit(data) # Fitting the data to the KMeans algorithm
                wcss.append(kmeans.inertia_)

            plt.plot(range(1, 11), wcss) # Creating the graph between WCSS and the number of clusters
            plt.title("The Elbow Method")
            plt.xlabel('Number of Clusters')
            plt.ylabel('WCSS')
            plt.savefig('apps/models/kmeans_elbow.png') # Saving the elbow plot locally
            # Finding the value of the optimum cluster programmatically
            self.kn = KneeLocator(range(1, 11), wcss, curve = 'convex', direction = 'decreasing')
            self.logger.info('The optimum number of clusters is: ' + str(self.kn.knee))
            self.logger.info("End of Elbow Plotting...")
            return self.kn.knee
        except Exception as e:
            self.logger.exception('Exception raised while elbow plotting: ' + str(e))
            raise Exception()


    def create_clusters(self, data, number_of_clusters):
        """
        * Method: create_clusters
        * Description: method to create clusters
        :return: A data frame with cluster column
        *
        * who       when        version     change (include bug = if apply)
        * -----     -------     -------     -------------------------------
        * Ray       04/02/2023  1.0         Initial Creation
        *
        * Parameters:
        *   data
        *   number_of_clusters
        """
        self.data = data
        try:
            self.logger.info('Start of Create Clusters...')
            self.kmeans = KMeans(n_clusters = number_of_clusters, init = 'k-means++', random_state = 0)
            self.y_kmeans = self.kmeans.fit_predict(data) # Divide data into clusters
            self.saveModel = self.fileOperation.save_model(self.kmeans, 'KMeans')
            # Saving the KMeans model to directory
            # Passing 'Model' as the functions need three parameters
            self.data['Cluster'] = self.y_kmeans # Create a new column in dataset for storing the cluster information
            self.logger.info('Successfully created ' + str(self.kn.knee) + 'clusters.')
            self.logger.info('End of Create Clusters...')
            return self.data
        except Exception as e:
            self.logger.exception('Exception raised while Creating Clusters: ' + str(e))
            raise Exception()

























