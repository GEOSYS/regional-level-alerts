from geosyspy import Geosys
import pandas
import logging
import datetime
import os
from geosyspy.utils.constants import *
from regionallevelalerts.utils import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class RegionalLevelAlerts():
    """
        Class to get weather or vegetation alerts on a regional entity (AMU) based on a defined parameter (weather or vegetation) and threshold.

        Parameters:
            - client_id (str): The client id
            - client_secret (str): The client secret
            - username (str): The api username
            - password (str): The api user password
            - enum_env (enum): 'Env.PROD' or 'Env.PREPROD'
            - enum_region (enum): 'Region.NA' or 'Region.EU'
            - priority_queue (str): 'realtime' (default) or 'bulk'
    """

    def __init__(self, client_id: str,
                 client_secret: str,
                 username: str,
                 password: str,
                 enum_env: enumerate,
                 enum_region: enumerate,
                 priority_queue: str = "realtime",
                 ):

        self.region: str = enum_region.value
        self.env: str = enum_env.value
        self.priority_queue: str = priority_queue
        self.__client: Geosys = Geosys(client_id, client_secret, username, password, enum_env, enum_region, priority_queue)


    def get_weather_alerts(self,
                           blockCode: AgriquestBlocks,
                           startDate: datetime.date,
                           endDate: datetime.date,
                           weatherType: AgriquestWeatherType,
                           threshold: float,
                           operator: NumbersComparisonOperator):
        """
            Get a pandas.DataFrame with regional entities in alert according to a weather type value, threshold and operator.
            - Calls GeosysPy function to get a DataFrame from Agriquest API with weather data on AMUs for a define period
            - According to the threshold and operator to apply (<, >, >=, <=), we add a column in the DataFrame "Is in alert" with True/False

            Args:
                - blockCode: Code of the Agriquest Block (Ex : FRA_DEPARTEMENTS)
                - startDate: beginning of the period
                - endDate: end of the period
                - weatherType: parameter to pass to Agriquest API (Ex : CUMULATIVE_PRECIPITATION, MAX_TEMPERATURE)
                - threshold: value to compare with
                - operator : operator to use for threshold comparison, among >, <, >=, <=

            Returns:
                pandas.DataFrame with regional entities weather data and their alert status
        """
        if operator not in [">", "<", ">=", "<="]:
            raise ValueError("get_weather_alerts: operator: possible values are >, <, >=, <=")

        # Get weather data from GeosysPy function
        logging.info(f"RegionalLevelAlerts:get_weather_alerts: {blockCode} - {str(startDate)} - {str(endDate)} - {weatherType} - threshold: {threshold} - operator: {operator}")
        df = self.__client.get_agriquest_weather_block_data(str(startDate), str(endDate), blockCode, weatherType)
        if df is None:
            return None

        # Define the function to apply to each value
        def compare_with_threshold(value):
            if operator == ">":
                return(float(value) > threshold)
            if operator == ">=":
                return(float(value) >= threshold)
            if operator == "<":
                return(float(value) < threshold)
            if operator == "<=":
                return(float(value) <= threshold)

        # Filter the DataFrame to keep only rows in alert. The Value column label contains the user unit so search column starting with "Value"
        value_column = [col for col in df.columns if col.startswith('Value')]
        mask = df[value_column[0]].apply(compare_with_threshold)
        initialNumberOfLines = df.shape[0]
        df = df[mask]
        finalNumberOfLines = df.shape[0]
        logging.info(f"RegionalLevelAlerts:get_weather_alerts: Filter the DataFrame to keep only rows in alert : {initialNumberOfLines} lines before filtering, {finalNumberOfLines} lines after filtering.")

        return df


    def get_vegetation_alerts(self,
                           blockCode: AgriquestBlocks,
                           observationDate: datetime.date,
                           threshold: float,
                           operator: NumbersComparisonOperator):
        """
            Get a pandas.DataFrame with regional entities in alert according to their NDVI value, threshold and operator.
            - Calls GeosysPy function to get a DataFrame from Agriquest API with NDVI data on AMUs for a given date
            - According to the threshold and operator to apply (<, >, >=, <=), we add a column in the DataFrame "Is in alert" with True/False

            Args:
                - blockCode: Code of the Agriquest Block (Ex : FRA_DEPARTEMENTS)
                - observationDate: date
                - threshold: value to compare with
                - operator : operator to use for threshold comparison, among >, <, >=, <=

            Returns:
                pandas.DataFrame with regional entities NDVI data and their alert status
        """
        if operator not in [">", "<", ">=", "<="]:
            raise ValueError("get_vegetation_alerts: operator: possible values are >, <, >=, <=")

        # Get vegetation data from GeosysPy function
        logging.info(f"RegionalLevelAlerts:get_vegetation_alerts: {blockCode} - {str(observationDate)} - threshold: {threshold} - operator: {operator}")
        df = self.__client.get_agriquest_ndvi_block_data(str(observationDate), blockCode, AgriquestCommodityCode.ALL_VEGETATION)
        if df is None:
            return None

        # Define the function to apply to each value
        def compare_with_threshold(value):
            if operator == ">":
                return(float(value) > threshold)
            if operator == ">=":
                return(float(value) >= threshold)
            if operator == "<":
                return(float(value) < threshold)
            if operator == "<=":
                return(float(value) <= threshold)

        # Filter the DataFrame to keep only rows in alert
        mask = df["NDVI"].apply(compare_with_threshold)
        initialNumberOfLines = df.shape[0]
        df = df[mask]
        finalNumberOfLines = df.shape[0]
        logging.info(f"RegionalLevelAlerts:get_vegetation_alerts: Filter the DataFrame to keep only rows in alert : {initialNumberOfLines} lines before filtering, {finalNumberOfLines} lines after filtering.")

        return df

