import numpy as np
import pandas as pd
from pprint import pp
from abc import ABC, abstractmethod
import json
from urllib.parse import quote
import urllib.request
import logging
from retry import retry
import typing
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import DataFrame as SparkDataFrame
import time
from random import randint, seed
from shapely.geometry import Point
import geopandas as gpd
import pycountry
import datetime

class ApiCall(ABC):
    """
    Declares the operations that all Api calls must implement
    """

    def __init__(self, qps_dict: dict = None, pois_limit: int = 20, saveloc: str = None, checkpoint: str = None, max_iter: int = None) -> None:
        """Parametrized constructor of the class.
        :param qps_dict: Dictionary with the following parameters to be set:
            qps: Number (int) of queries per second to the API. 
            n_retries: Number (int) of retries in case the call fails.
            factor: A factor (float) which determine how much percentage of the QPS is destine to do API calls 
            (the rest will be used for the retries)
            
        :type qps_dict: typing.Optional(dict, None)
        :param pois_limit: Limit for the number of POIs returned in the API response.
        :type pois_limit: typing.Optional(int, None)
        :param saveloc: Location where to store each pipeline results from Spark Streaming process.
        :type saveloc: str
        :param checkpoint: Location to store files that helps build fault-tolerant and resilient Spark applications.
        :type checkpoint: str
        :param max_iter: Integer that set the maximum amount of iteration in the loop to avoid Null values coming from the API respones.
        :type max_iter: int
        """
        
        # QPS for the spark streaming
        self.qps_dict = qps_dict
        # Spark session
        self.spark_session = SparkSession.builder.getOrCreate()
        # POIs limit for each call
        self.pois_limit = pois_limit
        self.saveloc = "/mnt/raw/pssr-metric/api2_pois_aux_auto_"
        self.checkpoint = "/mnt/raw/pssr-metric/checkpoint_auto_"
        self.max_iter = 1000


    @property
    @abstractmethod
    def api_key(self) -> str:
        """Key to get the access to the API"""
        pass


    @property
    @abstractmethod
    def provider_id(self) -> str:
        """Id to identify the provider"""
        pass


    @abstractmethod
    def get_geocode_request(self, address: str, limit: int = 100, country: str = None) -> str:
        """Constructs and returns the request for geocoding using a given address
        :param address: Address
        :type address: str
        :param limit: Limit of results, defaults to 100
        :type limit: int, optional
        :param country: Country, defaults to None
        :type country: str, optional
        :return: Request for geocoding
        :rtype: str
        """
        pass


    @abstractmethod
    def get_reverse_geocode_request(
      self, lat: typing.Union[str, float], lon: typing.Union[str, float], limit: int = 100, country: str = None
    ) -> str:
        """Constructs and returns the request for reverse geocoding using some given coordinates
        :param lat: Latitude
        :type lat: typing.Union[str, float]
        :param lon: Longitude
        :type lon: typing.Union[str, float]
        :param limit: Limit of results, defaults to 100
        :type limit: int, optional
        :param country: Country, defaults to None
        :type country: str, optional
        :return: Request for reverse geocoding
        :rtype: str
        """
        pass
      

    @abstractmethod
    def get_full_reverse_geocode_request(
      self, lat: typing.Union[str, float], lon: typing.Union[str, float], limit: int = 100, country: str = None, 
      added_responses: typing.List[str] = None, include_neighborhood: bool = False, verbose_placenames: bool = True
    ) -> str:
        """Constructs and returns the full request for reverse geocoding using some given coordinates. Most providers have a way of requesting 
        extra components from the reverse geocoding address. These method gets the extra components by adding them to the API call.
        :param lat: Latitude
        :type lat: typing.Union[str, float]
        :param lon: Longitude
        :type lon: typing.Union[str, float]
        :param limit: Limit of results, defaults to 100
        :type limit: int, optional
        :param country: Country, defaults to None
        :type country: str, optional
        :param added_responses: 
        :type added_responses: typing.List[str], optional
        :param include_neighborhood: 
        :type include_neighborhood:
        :param verbose_placenames:
        :type verbose_placenames:
        :return: Request for reverse geocoding
        :rtype: str
        """
        pass


    @abstractmethod
    def get_search_poi_request(self, param_dict: dict) -> str:
        """Constructs and returns the request for search POI using given dictionary with all the desired params
        :param param_dict: Dictionary with all the params to be included in the request
        :type param_dict: dict
        :return: Request query for poi search
        :rtype: str
        """

        pass


    @retry((urllib.request.HTTPError), tries=1, delay=1, backoff=2)
    def call_api(self, dict_params: dict, call_type: str) -> typing.Dict:
        """Method that make a call to the API given a endpoint type and returns a dictionary with the results
        :param dict_params: Dictionary with all the params in the call
        :type dict_params: dict
        :param call_type: String with the name of the endpoint type
        :type call_type: str
        :raises Exception: Error when selecting call: please, specify a correct call name (e.g. searchPoi, reverseGeocode, etc)
        :return: Dictionary with the results from call
        :rtype: typing.Dict
        """
        # Call to diferent endpoints given a call type
        if call_type == "poiSearch":
            logging.info("Calling searchPoi API end point...")
            
            try:
                httpurl = self.get_search_poi_request(dict_params)

            except TypeError as err:
                logging.error(f"TypeError: {err}")

        # TODO: Search by category (Nearby Search en google)
        # TODO: Search by brand

        elif call_type == "reverseGeocode": 
            #logging.info("Reverse Geocoding: lat={lat}, lon={lon}".format(lat=dict_params.get("lat"), lon=dict_params.get("lon")))

            try:
                httpurl = self.get_reverse_geocode_request(dict_params)
                
            except TypeError as err:
                logging.error(f"TypeError: {err}")
        
        elif call_type == "fullReverseGeocode":
            logging.info("Calling fullReverseGeocode API end point...")
            try:
                httpurl = self.get_full_reverse_geocode_request(dict_params)
                
            except TypeError as err:
                logging.error(f"TypeError: {err}")

        elif call_type == "geocode":
            #logging.info("Geocoding: address ={address}".format(address=dict_params.get("address")))
            try:
                httpurl = self.get_geocode_request(dict_params)
                
            except TypeError as err:
                logging.error(f"TypeError: {err}")

        else:
            raise Exception("Error when selecting call: please, specify a correct call name (e.g. poiSearch, reverseGeocode, etc)")

        data = None

        # Open the request
        with urllib.request.urlopen(httpurl) as url:
            # Parses JSON from the URL
            try:
                data = json.loads(url.read().decode())
            except json.decoder.JSONDecodeError:
                logging.error("Error while trying to convert the result to JSON")

        return data


    def set_qps(self):

        self.spark_session.sparkContext.setSystemProperty(
            "spark.streaming.receiver.maxRate", str(
              self.qps_dict.get("qps", 100) * (self.qps_dict.get("factor", 0,7) / self.qps_dict.get("n_retries")))
        )
        self.spark_session.sparkContext.setSystemProperty(
            "spark.streaming.backpressure.initialRate", str(
              self.qps_dict.get("qps", 100) * (self.qps_dict("factor", 0,7) / self.qps_dict.get("n_retries")))
        )
        self.spark_session.sparkContext.setSystemProperty("spark.streaming.backpressure.enabled", "true")

    def call_api_if_null(self, input_params: dict, call_type: str, last: dict) -> dict:
        """
        Call to TomTom's API with option to retry
        :param row: Array with relevant fields from the row
        :param limit: int limit in the number of results from TT API
        :param apiKey: str Api Key from TomTom
        :return: Array with the results
        """
        
        logging.info("LAST: ")
        logging.info(last)
        
        if last:
            # tt_api_results = callTTApiSearch(row, limit, apiKey)
            # Call to its API
            tt_api_results = self.call_api(input_params, call_type)
            
        else:
            tt_api_results = None
        
        return tt_api_results

    def udf_call_api(self, row: dict, last: dict) -> udf:
        udf_call = udf(
            lambda row, last: self.call_api_if_null(row, last, self.pois_limit, self.api_key), ArrayType(MapType(StringType(), StringType())))

        return udf_call

    def do_withColumn_api(self, original_step: SparkDataFrame) -> SparkDataFrame:
        """
        Do the call to TomTom's API and generate the spent number of api calls.
        :param original_step: dataframe First spark dataframe to work with (dataframe with the reference pois from Google)
        :return step_n_S: dataframe with the results of the TomTom API calls and the reference data from Google
        """
        
        # Call to the TomTom's API
        step_n_S = (original_step
            .withColumn('apiCalls2', when(col('apiResults').isNull(), col('apiCalls') + 1).otherwise(col('apiCalls')))
            .withColumn('apiResults2', when(col('apiResults').isNull(), self.udf_call_api(
                array(col("name"), col("lat"), col("lon"), col("search_radius_m").cast("string")), 
                col('apiResults').isNull())).otherwise(col('apiResults')))
                )
        
        # Droping old columns
        step_n_S = step_n_S.drop(*["apiResults", "apiCalls"])
        
        # Renaming columns
        step_n_S = step_n_S.withColumnRenamed("apiResults2", "apiResults")
        step_n_S = step_n_S.withColumnRenamed("apiCalls2", "apiCalls")
        
        return step_n_S

    def write_step(self, step, saveloc: str, checkpoint: str):
        """
        Void function
        """
        
        logging.info("Writing results to: {location}".format(location=saveloc))
        
        # Write stream
        streamQuery = (step
            .writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint)
            .option("path", saveloc)
            .outputMode("append")
            .trigger(once=True)  # To stop when it finishes the process
            .start()
        )
        
        logging.info("Waiting to write results...")
        logging.info("Num. of active streams: " + str(len(spark.streams.active)))
        
        # Waits for the streamQuery to finish processing
        streamQuery.awaitTermination()
        
        logging.info("Results has been written. Ready to be read")
        logging.info("Num. of active streams: " + str(len(spark.streams.active)))

    def do_streaming_api_calls_(self, input_data: SparkDataFrame) -> int:
        """Function that execute the process of calling the API desired using Spark Streaming, and writes the results in the datalake in a
        temporary file (saveloc_)
        :param input_data: Input data to be used as a reference to call the desired API.
        :type input_data: SparkDataFrame
        :return: Iteration number so it can be known how many iterations were needed to get zero nulls in the API response.
        :rtype: int
        """

        # Read reference POIs table
        # "/mnt/raw/pssr-metric/selected_reference_pois
        sdf_reference_pois = spark_utils.read_spark_dataframe(input_data, "delta")
  
        # Start chrono
        start = time.time()

        # Initialize two extra columns:
        #     apiResults: for the TomTom API
        #     apiCalss: for the number of retries for that particular reference POI
        sdf_reference_pois = (sdf_reference_pois
            .withColumn('apiResults', lit(None).cast( ArrayType(MapType(StringType(), StringType())) ))
            .withColumn('apiCalls',lit(0)))

        # Count number of reference POIs to be used to call the provider API
        n_reference = sdf_reference_pois.select([count(when(col("apiResults").isNull(), True))])
        logging.info("Number of reference POIs: {n_reference}".format(n_reference=n_reference))
        
        iteration = 1    # Iterations that index the steps done
        # all_results = False    # Indicator that tells if TomTom API call has all results (and not any Null)
        remaining = 1
        
        # Do calls to TomTom API until there's no null results
        while remaining > 0 and iteration <= self.max_iter:
            
            logging.info("ENTERING WHILE LOOP")
            logging.info("Iteration " + str(iteration))
            
            # Set iteration for the step
            checkpoint_ = self.checkpoint + str(iteration)
            saveloc_ = self.saveloc + str(iteration)
            logging.info("saveloc_: " + saveloc_)
            logging.info("checkpoint_: " + checkpoint_)

            # Call TomTom API for each reference POI
            sdf_api2_pois_iteration = self.do_withColumn_api(original_step=sdf_reference_pois, udf_call=udf_call)

            # Write results into the data lake
            self.write_step(sdf_api2_pois_iteration, saveloc_, checkpoint_)
            # Time spent in the writing
            end = time.time()
            time_elapsed = end - start
            logging.info("Total time elapsed: {0} seconds".format(time_elapsed))
            logging.info("Total time elapsed: {0} minutes".format(time_elapsed/60))
            
            logging.info("Starting to READ data...")
            
            # Read new step for next iteration
            sdf_api2_pois_iteration = spark_utils.readStream_spark_dataframe(self.saveloc, "delta")

            logging.info("Data read. Checking api results...")

            # Control while loop
            checker_sdf = spark_utils.read_spark_dataframe(saveloc_, "delta")
                
            # Adding column that set TomTom's API result status (1 = result given, 0 = Null result)
            checker_aux = checker_sdf.withColumn("api_status", when(col("apiResults").isNull(), 0).otherwise(1))#.withColumn("api_status2", when(col("apiResults2").isNull(), 0).otherwise(1))
            checker_aux.groupBy(["api_status", "apiCalls"]).count().show()
            
            # Remaining nulls in the TomTom's API results
            remaining = checker_sdf.filter(col("apiResults").isNull()).count()
            
            logging.info("EXITING WHILE LOOP (it "+str(iteration) + ", remaining " + str(remaining) + ")")
            
            # Increase iteration
            iteration = iteration + 1

        # TODO: Delete all temporary files created in each iteration.
        
        # Time spent in the whole process
        # Stop chrono to measure elapsed time
        end = time.time()
        # Time elapsed
        time_elapsed = end - start
        logging.info("Total time elapsed: {0} seconds".format(time_elapsed))
        logging.info("Total time elapsed: {0} minutes".format(time_elapsed/60))
        
        return iteration
  
    def read_api_results(self, iteration: int) -> SparkDataFrame:
        """Method that reads the results from the API call. It reads last iteration from the loop done in Spark Streaming to call the API
        :param iteration: Last iteration of the loop that calls the API until there are no Nulls in the responses.
        :type iteration: int
        :return: Spark Dataframe with all the results from the API.
        :rtype: SparkDataFrame
        """

        logging.info("Reading results from the API call. It reads last iteration from the loop done in Spark Streaming to call the API.")
        # Substracting one to the last iteration to get the actual last iteration (the counter starts at 1, that's why)
        saveloc_allsteps = self.saveloc_auto + str(iteration - 1)    # str(iteration - 1)
        logging.info(saveloc_allsteps)

        # Read last iteration (it has all the results)
        all_steps_RDD = spark_utils.read_spark_dataframe(saveloc_allsteps, "delta")

        return all_steps_RDD

    def sorting_api2_pois_columns(self, input_sdf: SparkDataFrame) -> SparkDataFrame:
        """Method that sort columns for the table api2_pois so it matches the structure of this table in the Data Lake.
        :param input_sdf: Input Spark Dataframe where all the final data for api2_pois table is.
        :type input_sdf: SparkDataFrame
        :return: Spark Dataframe with all the date for api2_pois table already sorted.
        :rtype: SparkDataFrame
        """

        output_sdf = input_sdf.select([
            "api2_run_id",
            "sample_id",
            "sample_run_id",
            "reference_id",
            "importance_rank",
            "search_rank",
            "search_request_string",
            "business_status",
            "name",
            "cat_subcat",
            "category",
            "subcategory",
            "ref_lat",
            "ref_lon",
            "search_radius_m",
            "location_distance_m",
            "search_group",
            "api_provider_id",
            "api_score",
            "api_search_rank",
            "api_poi_id",
            "api_lat",
            "api_lon",
            "api_countrySecondarySubdivision",
            "api_streetNumber",
            "api_postalCode",
            "api_municipality",
            "api_countrySubdivision",
            "api_localName",
            "api_streetName",
            "api_countryCodeISO3",
            "api_countryCode",
            "api_municipalitySubdivision",
            "api_freeformAddress",
            "api_poiName",
            "api_poiURL",
            "api_poiBrand",
            "api_poiPhone",
            "api_categoryId",
            "api_code",
            "api_results"
            ])
        
        return output_sdf

class TomtomApi(ApiCall):
    """
    Class that represent the TomTom API with the methods to call it
    """


    @property
    def api_key(self) -> str:
        """Key to get the access to the API"""
        return "QQxD2le5hPQanQAI1XYAAfaxWfyfpPaK"


    @property
    def provider_id(self) -> str:
        """Id to identify the provider"""
        return "tt"


    def get_geocode_request(self, param_dict: dict) -> str:
        """Constructs and returns the request for geocoding using given dictionary with all the desired params
        :param param_dict: Dictionary with all the params to be included in the request
        :type param_dict: dict
        :return: Request query for geocoding
        :rtype: str
        """
        address = param_dict.get("address")
        limit = param_dict.get("limit", 50)
        sleep = param_dict.get("sleep")
        if sleep is not None:
            time.sleep(sleep/2.5)
        country = param_dict.get("country", None)
        address_quoted = quote(address, safe='')  # url encode also with slashes /=%2F
        if country and country == "kr":
            return "https://kr-api.tomtom.com/search/2/geocode/"+address_quoted+".json?limit="+str(limit)+"&key="+self.api_key

        return "https://api.tomtom.com/search/2/geocode/"+address_quoted+".json?limit="+str(limit)+"&key="+self.api_key


    def get_reverse_geocode_request(self, param_dict: dict) -> str:
        """Constructs and returns the request for reverse geocoding using given dictionary with all the desired params
        :param param_dict: Dictionary with all the params to be included in the request
        :type param_dict: dict
        :return: Request query for reverse geocoding
        :rtype: str
        """
        lat = str(param_dict.get("lat"))
        lon = str(param_dict.get("lon"))
        limit = param_dict.get("limit", 50)
        country = param_dict.get("country", None)
        lat_lon = str(lat)+"%2C"+str(lon)
        if country and country == "kr":
            return "https://kr-api.tomtom.com/search/2/reverseGeocode/"+lat_lon+".json?limit="+str(limit)+"&key="+self.api_key

        return "https://api.tomtom.com/search/2/reverseGeocode/"+lat_lon+".json?limit="+str(limit)+"&key="+self.api_key
      
    ## TODO: Investigate if there is a way to get more components from this provider via API and add them to the method    
    def get_full_reverse_geocode_request(self, param_dict: dict) -> str:
        """Constructs and returns the request for reverse geocoding using given dictionary with all the desired params
        :param param_dict: Dictionary with all the params to be included in the request
        :type param_dict: dict
        :return: Request query for reverse geocoding
        :rtype: str
        """
        lat = str(param_dict.get("lat"))
        lon = str(param_dict.get("lon"))
        limit = param_dict.get("limit", 50)
        country = param_dict.get("country", None)
        lat_lon = str(lat)+"%2C"+str(lon)
        if country and country == "kr":
            return "https://kr-api.tomtom.com/search/2/reverseGeocode/"+lat_lon+".json?limit="+str(limit)+"&key="+self.api_key
          
        return "https://api.tomtom.com/search/2/reverseGeocode/"+lat_lon+".json?limit="+str(limit)+"&key="+self.api_key
        


    def get_search_poi_request(self, param_dict: dict) -> str:
        """Constructs and returns the request for search POI using given dictionary with all the desired params
        :param param_dict: Dictionary with all the params to be included in the request
        :type param_dict: dict
        :return: Request query for poi search
        :rtype: str
        """

        lat = str(param_dict.get("lat"))
        lon = str(param_dict.get("lon"))
        radius = str(param_dict.get("radius"))
        limit = param_dict.get("limit", 50)
        poi_name = quote(param_dict.get("name"), safe='')

        return "https://api.tomtom.com/search/2/poiSearch/" + poi_name + ".json?limit=" + str(limit) + "&openingHours=nextSevenDays&relatedPois=all&lat=" + lat + "&lon=" + lon + "&radius=" + radius + "&key="+self.api_key

    def extract_and_transform_api_result(self, iteration: int):

        # Read API results (from the Spark Streaming process)
        all_steps_RDD = self.read_api_results(iteration)

        # Transforming results from the API call:
        # Adding column that set TomTom's API result status (1 = result given, 0 = Null result)
        logging.info("Adding column that set TomTom's API result status (1 = result given, 0 = Null result)")
        all_steps_RDD_aux = all_steps_RDD.withColumn("api_status", when(col("apiResults").isNull(), 0).otherwise(1))

        # Exploding the results to get one row per TomTom API's result and keeping specific columns from reference POIs
        logging.info("Exploding the results to get one row per TomTom API's result and keeping specific columns from reference POIs...")
        step3_RDD = all_steps_RDD.select(
            "reference_id", 
            "sample_id", 
            "sample_run_id", 
            "importance_rank", 
            "classification", 
            "search_rank", 
            "search_request_string", 
            "business_status", 
            "name", 
            "lat", 
            "lon", 
            "search_radius_m", 
            "search_group", 
            posexplode_outer(col("apiResults"))
        )

        # Creating search_rank from the API response
        logging.info("Creating search_rank from the API response...")
        step3_RDD_ = (step3_RDD.withColumnRenamed("col", "api_results")
             .withColumnRenamed("pos", "api_search_rank"))

        # Trimming classification column and extracting category as well as subcategory for the reference POI
        logging.info("Trimming classification column and extracting category as well as subcategory for the reference POI...")
        # Trimming
        step3_RDD_1 = step3_RDD_.withColumn("first_trim", regexp_replace(col("classification"), '\\{"', ""))
        step3_RDD_2 = step3_RDD_1.withColumn("second_trim", regexp_replace(col("first_trim"), '"\\}', ""))
        step3_RDD_3 = step3_RDD_2.withColumn("third_trim", regexp_replace(col("second_trim"), '\\(', ""))
        step3_RDD_4 = step3_RDD_3.withColumn("fourth_trim", regexp_replace(col("third_trim"), '\\)', ""))
        step3_RDD_5 = step3_RDD_4.withColumn("fifth_trim", regexp_replace(col("fourth_trim"), '\\"', ""))

        # Spliting
        logging.info("Spliting column...")
        step3_RDD_6 = step3_RDD_5.withColumn("sixth_trim", split(col("fifth_trim"), ','))

        # Drop unnecesary columns
        logging.info("Drop unnecesary columns...")
        step3_RDD_7 = step3_RDD_6.drop(*["first_trim", "second_trim", "third_trim", "fourth_trim", "fifth_trim", "classification"])
        step3_RDD_8 = step3_RDD_7.withColumnRenamed("sixth_trim", "cat_subcat")

        # Extracting category and subcategory
        logging.info("Extracting category and subcategory...")
        step3_RDD_9 = step3_RDD_8.withColumn("category", col("cat_subcat")[0])
        step3_RDD_10 = step3_RDD_9.withColumn("subcategory", element_at(col("cat_subcat"), -1))

        # Extract columns from the TT API call result.
        # Getting first level of the TT API call JSON response.
        logging.info("Extract columns from the TT API call result.")
        logging.info("Getting first level of the TT API call JSON response...")
        step4_RDD = (step3_RDD_10
           .withColumn("tt_api_poi", from_json(col("api_results").getItem("poi"), MapType(StringType(), StringType())))
           .withColumn("tt_api_address", from_json(col("api_results").getItem("address"), MapType(StringType(), StringType())))
           .withColumn("tt_api_position", from_json(col("api_results").getItem("position"), MapType(StringType(), StringType())))
           .withColumn("tt_api_score", col("api_results").getItem("score"))
           .withColumn("tt_api_id", col("api_results").getItem("id"))
          )
        step5_RDD = (step4_RDD
           .withColumn("tt_api_categorySet", from_json(col("tt_api_poi").getItem("categorySet"), ArrayType(MapType(StringType(), StringType()))))
           .withColumn("tt_api_classifications", from_json(col("tt_api_poi").getItem("classifications"), ArrayType(MapType(StringType(), StringType()))))
           .withColumn("tt_api_brands", from_json(col("tt_api_poi").getItem("brands"), ArrayType(MapType(StringType(), StringType()))))
           .withColumn("tt_api_lat", col("tt_api_position").getItem("lat"))
           .withColumn("tt_api_lon", col("tt_api_position").getItem("lon"))
           .withColumn("tt_api_countrySecondarySubdivision", col("tt_api_address").getItem("countrySecondarySubdivision"))
           .withColumn("tt_api_streetNumber", col("tt_api_address").getItem("streetNumber"))
           .withColumn("tt_api_postalCode", col("tt_api_address").getItem("postalCode"))
           .withColumn("tt_api_municipality", col("tt_api_address").getItem("municipality"))
           .withColumn("tt_api_countrySubdivision", col("tt_api_address").getItem("countrySubdivision"))
           .withColumn("tt_api_localName", col("tt_api_address").getItem("localName"))
           .withColumn("tt_api_streetName", col("tt_api_address").getItem("streetName"))
           .withColumn("tt_api_countryCodeISO3", col("tt_api_address").getItem("countryCodeISO3"))
           .withColumn("tt_api_countryCode", col("tt_api_address").getItem("countryCode"))
           .withColumn("tt_api_municipalitySubdivision", col("tt_api_address").getItem("municipalitySubdivision"))
           .withColumn("tt_api_freeformAddress", col("tt_api_address").getItem("freeformAddress"))
           )
        step6_RDD = (step5_RDD
           .withColumn("tt_api_poiName", col("tt_api_poi").getItem("name"))
           .withColumn("tt_api_poiURL", col("tt_api_poi").getItem("url"))
           .withColumn("tt_api_poiBrand", col("tt_api_brands").getItem(0).getItem("name"))
           .withColumn("tt_api_poiPhone", col("tt_api_poi").getItem("phone"))
           .withColumn("tt_api_categoryId", col("tt_api_categorySet").getItem(0).getItem("id"))
           .withColumn("tt_api_code", col("tt_api_classifications").getItem(0).getItem("code"))
          )

        logging.info("Getting just the columns that are needed...")
        # Columns to be dropped.
        columns_to_drop = [
            "tt_api_poi",
            "tt_api_address",
            "tt_api_position",
            "tt_api_categorySet",
            "tt_api_classifications",
            "tt_api_brands"
            ]

        step7_RDD = step6_RDD.drop(*columns_to_drop)

        logging.warning("Renaming columns...")
        # Renaming and adding some extra columns that are needed
        step8_RDD = (step7_RDD
           .withColumnRenamed("lat", "ref_lat")
           .withColumnRenamed("lon", "ref_lon")
           .withColumnRenamed("tt_api_id", "api_poi_id")
           .withColumnRenamed("tt_api_lat", "api_lat")
           .withColumnRenamed("tt_api_lon", "api_lon")
           .withColumnRenamed("tt_category_id", "ref_in_tt_category_id")
           .withColumnRenamed("tt_api_countrySecondarySubdivision", "api_countrySecondarySubdivision")
           .withColumnRenamed("tt_api_streetNumber", "api_streetNumber")
           .withColumnRenamed("tt_api_postalCode", "api_postalCode")
           .withColumnRenamed("tt_api_municipality", "api_municipality")
           .withColumnRenamed("tt_api_countrySubdivision", "api_countrySubdivision")
           .withColumnRenamed("tt_api_localName", "api_localName")
           .withColumnRenamed("tt_api_streetName", "api_streetName")
           .withColumnRenamed("tt_api_countryCodeISO3", "api_countryCodeISO3")
           .withColumnRenamed("tt_api_countryCode", "api_countryCode")
           .withColumnRenamed("tt_api_municipalitySubdivision", "api_municipalitySubdivision")
           .withColumnRenamed("tt_api_freeformAddress", "api_freeformAddress")
           .withColumnRenamed("tt_api_poiName", "api_poiName")
           .withColumnRenamed("tt_api_poiURL", "api_poiURL")
           .withColumnRenamed("tt_api_poiBrand", "api_poiBrand")
           .withColumnRenamed("tt_api_poiPhone", "api_poiPhone")
           .withColumnRenamed("tt_api_categoryId", "api_categoryId")
           .withColumnRenamed("tt_api_code", "api_code")
           .withColumnRenamed("tt_api_score", "api_score")
           .withColumn("api_provider_id", lit("tt"))    # this set ups the provider that has been called (API call)
           .withColumn("api2_run_id", lit(None))
          )
        
        return step8_RDD


def run_api_calls(
    df, country, two_letter_provider_id='tt', credentials={'host': '10.137.173.74', 'database': 'ggg', 'user': 'ggg', 'password': 'ok'},
    inspect: bool = False
):
    """Function that performs API calls on the DataFrame you pass using the provider declared with the two letter provider id code you pass as an argument. If no argument for provider id is passed, it will run on TomTom Genesis by default. The DataFrame has to have a column named 'lat' and a column named 'lon' for this function to work or else it will raise a NameError!

    :param df: Pandas DataFrame that contains the latitude and longitude in 
    :type df: pd.DataFrame
    :param two_letter_provider_id:
    :type two_letter_provider_id:
    :param country: Three-letter iso country code
    :type country: str
    :param credentials: (Deprecated) Dictionary containing the credentials to connect to the virtual machine where the updated version of Orbis is kept. Defaults to None.
    :type credentials: dict
    :param inspect: Boolean that shows the collected latitude and longitude that we are passing to the API, one-by-one. Only works for tt, he, gg and bg.
    :type inspect: bool
    :return: The dataframe that contains the response for the API call on the provider in a column named 'results' and the provider_id in a column named 'provider_id'.
    :rtype: DataFrame
    """
    provider_list = ('tt', 'he', 'gg', 'bg', 'os', 'or', 'mnr')
    
    if two_letter_provider_id not in provider_list:
        raise NameError(f'The provider_id should be one of the folloing: {provider_list}. You passed: {two_letter_provider_id}')
    
    if (('lat' not in df.columns) and ('lon' not in df.columns)):
        raise KeyError('Your dataframe does not contain a column named "lat" or a column named "lon", please verify the columns')

    df_copy = df.copy()

    if two_letter_provider_id == 'tt':
        api = TomtomApi()
        def function(x):
            lat, lon = x['lat'], x['lon']
            if inspect:
                print(f'collected lat: "{lat}" and lon: "{lon}"')
            return api.call_api({'lat': lat, 'lon': lon, 'limit': 1}, 'fullReverseGeocode')

    elif two_letter_provider_id == 'he':
        api = HereApi()
        def function(x):
            lat, lon = x['lat'], x['lon']
            if inspect:
                print(f'collected lat: "{lat}" and lon: "{lon}"')
            return api.call_api({'lat': lat, 'lon': lon, 'sleep': 1.5}, 'fullReverseGeocode')

    elif two_letter_provider_id == 'gg':
        api = GoogleApi()
        def function(x):
            lat, lon = x['lat'], x['lon']
            if inspect:
                print(f'collected lat: "{lat}" and lon: "{lon}"')
            return api.call_api({'lat': lat, 'lon': lon, 'sleep': 1}, 'fullReverseGeocode')

    elif two_letter_provider_id == 'bg':
        api = BingApi()
        def function(x):
            lat, lon = x['lat'], x['lon']
            if inspect:
                print(f'collected lat: "{lat}" and lon: "{lon}"')
            return api.call_api(
                    {
                        'lat': x['lat'], 'lon': x['lon'], 'limit': 1, 'include_neighborhood': True, 'sleep': 1,
                        'added_responses': ['Address', 'Neighborhood', 'PopulatedPlace', 'Postcode1', 'AdminDivision1', 'AdminDivision2', 
                                            'CountryRegion']
                    }, 'fullReverseGeocode'
                )            
        
    # Functions for OSM, ORBIS and MNR are done at DF level
    # If the provider is one of these, the process doesn't need an apply:
    elif two_letter_provider_id == 'mnr':
        df_copy['coordinates'] = df_copy.apply(lambda x: Point(x.lon, x.lat), axis=1)
        country = df_copy['country'].iloc[0]
        schema = map_content.utils.mnr.find_country_schemas(country, latest=True)
        geo_df = gpd.GeoDataFrame(df_copy, geometry='coordinates')
        mnr_result_components = lookup_address_components(geo_df['coordinates'], schema)
        mnr_result_components = refactor_mnr_response(mnr_result_components)
        
        df_copy['results'] = mnr_result_components.apply(lambda x: x.to_dict(), axis=1)
        
    elif two_letter_provider_id == 'os':
        df_copy['coordinates'] = df_copy.apply(lambda x: Point(x.lon, x.lat), axis=1)
        country = df_copy['country'].iloc[0]
        osm_schema = find_osm_schema(country)
        geo_df = gpd.GeoDataFrame(df_copy, geometry='coordinates')
        osm_result_components = lookup_osm_components(geo_df['coordinates'], osm_schema)
        
        df_copy['results'] = osm_result_components.apply(lambda x: x.to_dict(), axis=1)
        
    elif two_letter_provider_id == 'or':
        df_copy['coordinates'] = df_copy.apply(lambda x: Point(x.lon, x.lat), axis=1)
        country = df_copy['country'].iloc[0]
        orbis_schema = find_openmap_schema(country, credentials=credentials)
        geo_df = gpd.GeoDataFrame(df_copy, geometry='coordinates')
        orbis_result_components = lookup_openmap_components(geo_df['coordinates'], orbis_schema)
        
        df_copy['results'] = orbis_result_components.apply(lambda x: x.to_dict(), axis=1)

        
    if two_letter_provider_id not in ('mnr', 'or', 'os'):    
        df_copy['results'] = df_copy.apply(function, axis=1)
        
    df_copy['provider_id'] = two_letter_provider_id

    return df_copy


def run_api_calls_on_all_providers(
    df: pd.DataFrame, country_iso3: str, provider_list: list = ('tt', 'he', 'os', 'or', 'gg', 'bg', 'mnr'), 
    credentials: dict = {'host': '10.137.173.74', 'database': 'ggg', 'user': 'ggg', 'password': 'ok'}, inspect: bool = False
) -> pd.DataFrame:
    """Function that runs the API calls process on all providers and returns a DataFrame that contatenates the results. The process runs over the DataFrame that contains the ASF sample with the searched query, lat and lon components for each element of the sample.
    
    :param df: DataFrame that contains the ASF sample with the searched query, lat and lon components of the sample.
    :type df: pd.DataFrame
    :param country_iso3: Country ISO code in three-letter format
    :type country_iso3: str
    :param provider_list: List of provider ids to pass to the function. Defaults to ('tt', 'he', 'os', 'or', 'gg', 'bg', 'mnr')
    :type provider_list: list
    :param credentials: (Deprecated) Dictionary containing the credentials to connect to the virtual machine where the updated version of Orbis is kept. Defaults to None.
    :type credentials: dict
    :param inspect: Boolean that shows the collected latitude and longitude that we are passing to the API, one-by-one. Only works for tt, he, gg and bg.
    :type inspect: bool
    :return: A DataFrame that contains all the API calls performed on all the providers in the list.
    :rtype: pd.DataFrame
    """
    api_calls = pd.DataFrame()
    
    for provider in provider_list:
        single_provider_calls = run_api_calls(df, country_iso3, provider, credentials=credentials, inspect=inspect)
        api_calls = pd.concat([api_calls, single_provider_calls])
        
        if provider == 'tt':
            print('Finished with TT\n\n\n\n\n')
            
        elif provider == 'he':
            print('Finished with HE\n\n\n\n\n')
        
        elif provider == 'os':
            print('Finished with OSM\n\n\n\n\n')
            
        elif provider == 'or':
            print('Finished with ORBIS\n\n\n\n\n')
            
        elif provider == 'gg':
            print('Finished with GG\n\n\n\n\n')
            
        elif provider == 'bg':
            print('Finished with BG\n\n\n\n\n')
            
        elif provider == 'mnr':
            print('Finished with MNR\n\n\n\n\n')
    
    return api_calls