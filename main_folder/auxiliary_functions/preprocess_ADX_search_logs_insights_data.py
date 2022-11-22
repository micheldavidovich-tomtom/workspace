# Databricks notebook source
#! pip install pycountry
#! pip install country_list
#! pip install country_converter

# COMMAND ----------

import pandas as pd
import numpy as np
import pycountry
from maps_analytics_utils.connections import adx, connections_utils
from country_list import countries_for_language, available_languages
import typing
import country_converter
import json
import re
import matplotlib.pyplot as plt
from datetime import date
import regex

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parsing the libpostal response:

# COMMAND ----------

def get_libpostal_condition(component, possible_results):
    '''
    :param possible_results: These are the possible outcomes that this value may have. For example a house number can contain spaces, letters and numbers (think of calle de Goya 23 b --> house number is: "23 b")
    :type possible_results: str
    '''
    libpostal_condition = f'"{component}":"{possible_results}"' + '[,\}]'
    
    return libpostal_condition

def parse_libpostal(df: pd.DataFrame) -> pd.DataFrame:
    ''' Function that parses the Libpostal components and returns a DataFrame with a column for each parsed component in the Libpostal dictionary, mapped to a component in the TomTom documentation, if there is one. If not, we keep the Libpostal component. The DataFrame must contain a column called lib_postal_result, that includes the dictionary in string format of the Libpostal response.
    
    :param df: DataFrame containing the Libpostal response in a str(dict) format from which you want to extract the components.
    :type df: pd.DataFrame
    :return: Returns the same DataFrame with the added columns. It will contain 19 extra columns that parse the entire set of component options from the Libpotal response.
    :rtype: pd.DataFrame
    '''
    results = df.copy()
    
    results['removed_air_quotes'] = results['lib_postal_result'].map(lambda x: re.sub('"+', '"', re.sub("'+", "'", x)))
    
    ### house:
    house_condition = get_libpostal_condition('house', '(.+?)(?=")')
    results['lp_building_name'] = (   ## Building name is the term used in the TT documentation
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(house_condition, x) is None 
                          else regex.search(house_condition, x).group(1))
    )
    
    ### near:
    near_condition = get_libpostal_condition('near', '(.+?)(?=")')
    results['lp_near'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(near_condition, x) is None 
                          else regex.search(near_condition, x).group(1))
    )
    
    ### house_number:
    house_number_condition = get_libpostal_condition('house_number', '(.+?)(?=")')
    results['lp_house_number'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(house_number_condition, x) is None 
                          else regex.search(house_number_condition, x).group(1))
    )
    
    ### road:
    road_condition = get_libpostal_condition('road', '(.+?)(?=")')
    results['lp_road'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(road_condition, x) is None 
                          else regex.search(road_condition, x).group(1))
    )
    
    ### unit:
    unit_condition = get_libpostal_condition('unit', '(.+?)(?=")')
    results['lp_unit'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(unit_condition, x) is None 
                          else regex.search(unit_condition, x).group(1))
    )
    
    ### level:
    level_condition = get_libpostal_condition('level', '(.+?)(?=")')
    results['lp_floor'] = (    ## level is the equivalent of floor in the TT documentation ("Ground floor, 3rd floor, etc..")
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(level_condition, x) is None 
                          else regex.search(level_condition, x).group(1))
    )
    
    ### staircase:
    staircase_condition = get_libpostal_condition('staircase', '(.+?)(?=")')
    results['lp_staircase'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(staircase_condition, x) is None 
                          else regex.search(staircase_condition, x).group(1))
    )
    
    ### entrance:
    entrance_condition = get_libpostal_condition('entrance', '(.+?)(?=")')
    results['lp_entrance'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(entrance_condition, x) is None 
                          else regex.search(entrance_condition, x).group(1))
    )
    
    ### postcode:
    postal_code_condition = get_libpostal_condition('postcode', '(.+?)(?=")')
    results['lp_postal_code'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(postal_code_condition, x) is None 
                          else regex.search(postal_code_condition, x).group(1))
    )
    
    ### po_box:
    po_box_condition = get_libpostal_condition('po_box', '(.+?)(?=")')
    results['lp_po_box'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(po_box_condition, x) is None 
                          else regex.search(po_box_condition, x).group(1))
    )
    
    ### suburb:
    suburb_condition = get_libpostal_condition('suburb', '(.+?)(?=")')
    results['lp_suburb'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(suburb_condition, x) is None 
                          else regex.search(suburb_condition, x).group(1))
    )
    
    ### city_district:
    city_district_condition = get_libpostal_condition('city_district', '(.+?)(?=")')
    results['lp_city_district'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(city_district_condition, x) is None 
                          else regex.search(city_district_condition, x).group(1))
    )
    
    ### city:
    city_condition = get_libpostal_condition('city', '(.+?)(?=")')
    results['lp_city'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(city_condition, x) is None 
                          else regex.search(city_condition, x).group(1))
    )
    
    ### island:
    island_condition = get_libpostal_condition('island', '(.+?)(?=")')
    results['lp_island'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(island_condition, x) is None 
                          else regex.search(island_condition, x).group(1))
    )
    
    ### state_district:
    state_district_condition = get_libpostal_condition('state_district', '(.+?)(?=")')
    results['lp_state_district'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(state_district_condition, x) is None 
                          else regex.search(state_district_condition, x).group(1))
    )
    
    ### state:
    state_condition = get_libpostal_condition('state', '(.+?)(?=")')
    results['lp_state'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(state_condition, x) is None 
                          else regex.search(state_condition, x).group(1))
    )
    
    ### country_region
    country_region_condition = get_libpostal_condition('country_region', '(.+?)(?=")')
    results['lp_country_region'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(country_region_condition, x) is None 
                          else regex.search(country_region_condition, x).group(1))
    )
    
    ### country:
    country_condition = get_libpostal_condition('country', '(.+?)(?=")')
    results['lp_country'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(country_condition, x) is None 
                          else regex.search(country_condition, x).group(1))
    )
    
    ### world_region:
    world_region_condition = get_libpostal_condition('world_region', '(.+?)(?=")')
    results['lp_world_region'] = (
        results['removed_air_quotes']
        .map(lambda x: '' if regex.search(world_region_condition, x) is None 
                          else regex.search(world_region_condition, x).group(1))
    )
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parsing structured Geocode entries

# COMMAND ----------

def get_structGeo_condition(component, possible_results):
    '''Function that converts the component and possible_results passed to a query that filters the information from the structuredGeocode url between the quotes.
    
    :param component: TT component on which to look for the "possible_results" condition.
    :type component: str
    :param possible_results: Regex condition that is expected to be passed to the get_s2sG_results function in order to parse the components from the search 2 structuredGeocode endpoint. These are the possible outcomes that this value may have. For example a house number can contain spaces, letters and numbers (think of calle de Goya 23 b --> house number is: "23 b")
    :type possible_results: str
    '''
    structGeo_condition = f'"{component}":"{possible_results}"' + '[,\}]'
    
    return structGeo_condition

def get_s2sG_results(sdf) -> pd.DataFrame:
    '''Function that gets a DataFrame that contains the information on the parsed_request_quertystring column and returns a dataframe with all the parsed data.
    
    :param sdf: DataFrame that conatins all the responses from the ADX query
    :type sdf: pd.DataFrame
    :return: DataFrame with the parsed data in the structuredGeocode responses.
    :rtype: pd.DataFrame
    '''
    df = sdf.copy()
    
    ## Creating filter:
    s2sg_filter = (df['method_name'] == 'search 2 structuredGeocode')
    
    ## Removing multiple air quotes from the parsed query string:
    df['removed_air_quotes'] = df['parsed_request_quertystring'].map(lambda x: re.sub('"+', '"', re.sub("'+", "'", x)))
    
    ## Parsing streetNumber:
    house_number_condition = get_structGeo_condition('streetNumber', '(.+?)(?=")')
    df.loc[s2sg_filter, 'lp_house_number'] = df.loc[s2sg_filter, 'removed_air_quotes'].map(
        lambda x: '' if regex.search(house_number_condition, x) is None 
        else regex.search(house_number_condition, x).group(1)
    )
    
    ## Parsing streetName:
    road_condition = get_structGeo_condition('streetName', '(.+?)(?=")')
    df.loc[s2sg_filter, 'lp_road'] = df.loc[s2sg_filter, 'removed_air_quotes'].map(
        lambda x: '' if regex.search(road_condition, x) is None 
        else regex.search(road_condition, x).group(1)
    )
    
    ## Parsing postalCode:
    postal_code_condition = get_structGeo_condition('postalCode', '(.+?)(?=")')
    df.loc[s2sg_filter, 'lp_postal_code'] = df.loc[s2sg_filter, 'removed_air_quotes'].map(
        lambda x: '' if regex.search(postal_code_condition, x) is None 
        else regex.search(postal_code_condition, x).group(1)
    )
    
    ## Parsing municipalitySubdivision:
    suburb_condition = get_structGeo_condition('municipalitySubdivision', '(.+?)(?=")')
    df.loc[s2sg_filter, 'lp_suburb'] = df.loc[s2sg_filter, 'removed_air_quotes'].map(
        lambda x: '' if regex.search(suburb_condition, x) is None
        else regex.search(suburb_condition, x).group(1)
    )
    
    ### NOTE ON CITY DISTRICT: The only component that maps well to this is municipalitySubdivision which also maps to suburb.
    ### We are keeping suburb and removing city district. If we keep both daat will be repeated.
    ## Parsing city_district:
    #city_district_condition = get_structGeo_condition('municipalitySubdivision', '(.+?)(?=")')
    #df['lp_city_district'] = df['removed_air_quotes'].map(lambda x: '' if regex.search(city_district_condition, x) is None 
    #                      else regex.search(city_district_condition, x).group(1))
    
    ## Parsing municipality:
    city_condition = get_structGeo_condition('municipality', '(.+?)(?=")')
    df.loc[s2sg_filter, 'lp_city'] = df.loc[s2sg_filter, 'removed_air_quotes'].map(
        lambda x: '' if regex.search(city_condition, x) is None 
        else regex.search(city_condition, x).group(1)
    )
    
    ## Parsing countrySecondarySubdivision:
    state_district_condition = get_structGeo_condition('countrySecondarySubdivision', '(.+?)(?=")')
    df.loc[s2sg_filter, 'lp_state_district'] = df.loc[s2sg_filter, 'removed_air_quotes'].map(
        lambda x: '' if regex.search(state_district_condition, x) is None 
        else regex.search(state_district_condition, x).group(1)
    )
    
    ## Parsing countrySubdivision:
    state_condition = get_structGeo_condition('countrySubdivision', '(.+?)(?=")')
    df.loc[s2sg_filter, 'lp_state'] = df.loc[s2sg_filter, 'removed_air_quotes'].map(
        lambda x: '' if regex.search(state_condition, x) is None 
        else regex.search(state_condition, x).group(1)
    )
    
    ## Parsing countryCode:
    country_condition = get_structGeo_condition('countryCode', '(.+?)(?=")')
    df.loc[s2sg_filter, 'lp_country'] = df.loc[s2sg_filter, 'removed_air_quotes'].map(
        lambda x: '' if regex.search(country_condition, x) is None 
        else regex.search(country_condition, x).group(1)
    )

    ## Creating the searched queries:
    total_columns = [
        'lp_building_name', 'lp_near', 'lp_house_number', 'lp_road', 'lp_unit', 'lp_floor', 'lp_staircase', 
        'lp_entrance', 'lp_postal_code', 'lp_po_box', 'lp_suburb', 'lp_city_district', 'lp_city', 'lp_island',
        'lp_state_district', 'lp_state', 'lp_country_region', 'lp_country'
    ]

    df.loc[s2sg_filter, 'searched_query'] = df.loc[s2sg_filter, total_columns].apply(
        lambda x: re.sub('\s+',' ', ' '.join(x[total_columns].values).rstrip().lstrip().replace('%20', ' ')), axis=1
    )
    
    return df

# COMMAND ----------

def parse_url_string(string: str, url_mapping_dict: dict = {'%2C': ',', '%20': ' ', '%3A': ':', '%27': "'"}) -> str:
    """Function that gets a url or string in url format and converts the strings into unicode using the mapping dict that you pass.
    
    :param string: The string you want to correct using the mapping dict.
    :type string: str
    :param url_mapping_dict: A dictionary that contains the url format to change as keys and the unicode response as values, defaults to {'%2C': ',', '%20': ' ', '%3A': ':', '%27': "'"}
    :type url_mapping_dict: dict, optional
    :return: The parsed string with unidecode characters
    :rtype: str
    """
    for url_component in url_mapping_dict.keys():
        replacement = url_mapping_dict[url_component]
        string = string.replace(url_component, replacement)
    
    return string

def convert_structured_geocode_to_search_query(
    row: pd.Series, mapping_dict: dict = {'%2C': ',', '%20': ' ', '%3A': ':', '%27': "'"}, subdivision_list: list or tuple = [
        'lp_house_number', 'lp_road', 'lp_postal_code', 'lp_suburb', 'lp_city', 'lp_state_district', 'lp_state', 'lp_country'
    ]
) -> str:
    """Function that generates an artificial searched query based on the inputs the user gave in the structured geocode. The dataframe must have been already parsed using the get_s2sG_results to accurately work!
    
    :param row: Series that has been parsed with the get_s2sG_results to generate the house_number, street_number, locality, etc., components. The process uses these components to build the "searched query"
    :type df: pd.Series
    :param mapping_dict: A dictionary that contains the url format to change as keys and the unicode response as values, defaults to {'%2C': ',', '%20': ' ', '%3A': ':', '%27': "'"}
    :type mapping_dict: dict, optional
    :param subdivision_list: List or Tuple containing the different subdivisions we should use in order to generate the artificial searched query. These components are the ones that you generate from the get_s2sG_results process, so generally you shouldn't be changing these parameter much.
    :type subdivision_list: list or tuple
    :return: The artificial searched query. This is not exactly a searched query as the user didn't pass this exactly, but it will do for now.
    :rtype: str
    """
    only_components_row = row[subdivision_list]
    num_components = len(subdivision_list)

    if (only_components_row == '').sum() == num_components:
        return np.nan
    else:
        searched_query = ''
        read_list = [parse_url_string(row[element], mapping_dict) for element in subdivision_list if row[element] != '']

        return ', '.join(read_list)
    
def generate_searched_query_on_full_dataframe(
    df: pd.DataFrame, mapping_dict: dict = {'%2C': ',', '%20': ' ', '%3A': ':', '%27': "'"}, subdivision_list: list or tuple = [
        'lp_house_number', 'lp_road', 'lp_postal_code', 'lp_suburb', 'lp_city', 'lp_state_district', 'lp_state', 'lp_country'
    ]
) -> pd.DataFrame:
    """Function that generates an artificial searched query based on the inputs the user gave in the structured geocode. The dataframe must have been already parsed using the get_s2sG_results to accurately work!
    
    :param df: DataFrame that has been parsed with the get_s2sG_results to generate the house_number, street_number, locality, etc., components. The process uses these components to build the "searched query"
    :type df: pd.DataFrame
    :param mapping_dict: A dictionary that contains the url format to change as keys and the unicode response as values, defaults to {'%2C': ',', '%20': ' ', '%3A': ':', '%27': "'"}
    :type mapping_dict: dict, optional
    :param subdivision_list: List or Tuple containing the different subdivisions we should use in order to generate the artificial searched query. These components are the ones that you generate from the get_s2sG_results process, so generally you shouldn't be changing these parameter much.
    :type subdivision_list: list or tuple
    :return: A copy of the DataFrame with the artificial searched query generated. This is not exactly a searched query as the user didn't pass this exactly, but it will do for now.
    :rtype: pd.DataFrame
    """
    df_copy = df.copy()
    
    df_copy.loc[df_copy['method_name'] == 'search 2 structuredGeocode', 'searched_query'] = (
        df_copy.loc[df_copy['method_name'] == 'search 2 structuredGeocode', :].apply(lambda x: convert_structured_geocode_to_search_query(x, mapping_dict, subdivision_list), axis=1)
    )
    
    return df_copy

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Generating the results columns:

# COMMAND ----------

def parse_address_components(country_df: pd.DataFrame) -> pd.DataFrame:
    """Function that receives a DataFrame containing the sample obtained from the "search_logs_insights" table and returns a DataFrame with the parsed addresses for counts. The response DataFrame will contain the following boolean columns to use for counting: 
    - has_house
    - has_house_num
    - has_road
    - has_unit
    - has_postal_code
    - has_locality
    - has_city
    - has_state
    - has_country

    :param country_df: DataFrame containing the sample from the "search_logs_insights" or similar table. The table should at least contain these columns: 'house', 'house_number', 'road', 'unit', 'postcode', 'suburb', 'city_district', 'city', 'state', 'state_district' and 'country'
    :type country_df: pd.DataFrame
    :return: Returns a DataFrame with the columns stated above.
    :rtype: pd.DataFrame
    """
    response = country_df.copy()
    
    empty_condition = ('', np.nan, 'False', ' ')
    
    response['has_building_name'] = response['lp_building_name'].map(lambda x: True if x not in empty_condition else False)
    response['has_near'] = response['lp_near'].map(lambda x: True if x not in empty_condition else False)
    response['has_house_num'] = response['lp_house_number'].map(lambda x: True if x not in empty_condition else False)
    response['has_road'] = response['lp_road'].map(lambda x: True if x not in empty_condition else False)
    response['has_unit'] = response['lp_unit'].map(lambda x: True if x not in empty_condition else False)
    response['has_floor'] = response['lp_floor'].map(lambda x: True if x not in empty_condition else False)
    response['has_staircase'] = response['lp_staircase'].map(lambda x: True if x not in empty_condition else False)
    response['has_entrance'] = response['lp_entrance'].map(lambda x: True if x not in empty_condition else False)
    response['has_postal_code'] = response['lp_postal_code'].map(lambda x: True if x not in empty_condition else False)
    response['has_po_box'] = response['lp_po_box'].map(lambda x: True if x not in empty_condition else False)
    response['has_suburb'] = response['lp_suburb'].map(lambda x: True if x not in empty_condition else False)
    response['has_city_district'] = response['lp_city_district'].map(lambda x: True if x not in empty_condition else False)
    response['has_city'] = response['lp_city'].map(lambda x: True if x not in empty_condition else False)
    response['has_island'] = response['lp_island'].map(lambda x: True if x not in empty_condition else False)
    response['has_state_district'] = response['lp_state_district'].map(lambda x: True if x not in empty_condition else False)
    response['has_state'] = response['lp_state'].map(lambda x: True if x not in empty_condition else False)
    response['has_country'] = response['lp_country'].map(lambda x: True if x not in empty_condition else False)
    response['has_country_region'] = response['lp_country_region'].map(lambda x: True if x not in empty_condition else False)
    response['has_world_region'] = response['lp_world_region'].map(lambda x: True if x not in empty_condition else False)
    
    return response

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parsing all together

# COMMAND ----------

def extract_format_from_path(path: str) -> str:
    """Function that extracts the file from a path that contains the format at the end and no other points in the file name.
    
    :param path: Path where the data is stored. It will extract the format by getting all the letter after the only point in the path.
    :type path: str
    :return: The format in string format (for example 'csv' or 'parquet')
    :rtype: str
    """
    match = re.match('.+\.([\w]+)', path)
    
    if match is None:
        print('No path extracted')
    else:
        return match.group(1)
    
def parse_libpostal_and_structured_geocode(
    path: str, sep: str = ';', header: bool = True, mapping_dict: dict = {'%2C': ',', '%20': ' ', '%3A': ':', '%27': "'", '%28': '(', '%29': ')'}, 
    subdivision_list: list or tuple = [
        'lp_house_number', 'lp_road', 'lp_postal_code', 'lp_suburb', 'lp_city', 'lp_state_district', 'lp_state', 'lp_country'
    ]
) -> pd.DataFrame:
    """Function that gets the DataFrame and parses the libpostal responses and the structured geocoding responses. It basically returns a DataFrame with all the relevant data parsed!
    
    :param df: DataFrame that contains the data as obtained from the sampling process in the search_logs_insights table in ADX.
    :type df: pd.DataFrame
    :param path: Specifies the path from which to read the data.
    :type path: str
    :param sep: Separator in case you use the csv format, defaults to ';'
    :type sep: str or None, optional
    :param header: Whether you want the first row of the csv to be the columns of the DataFrame, defaults to True, which means the first row of the csv has the column names for the DataFrame.
    :type header: bool, optional
    :param mapping_dict: A dictionary that contains the url format to change as keys and the unicode response as values, defaults to {'%2C': ',', '%20': ' ', '%3A': ':', '%27': "'"}
    :type mapping_dict: dict, optional
    :param subdivision_list: List or Tuple containing the different subdivisions we should use in order to generate the artificial searched query. These components are the ones that you generate from the get_s2sG_results process, so generally you shouldn't be changing these parameter much.
    :type subdivision_list: list or tuple
    :return: A DataFrame with the parsed relevant responses
    :rtype: pd.DataFrame
    """
    file_format = extract_format_from_path(path)
    
    if file_format == 'csv':
        df = pd.read_csv(path, sep=sep, header=header)
    elif file_format == 'parquet':
        df = pd.read_parquet(path)
    else:
        raise TypeError('The format is not allowed of the file path is incorrect, check the file is csv or parquet, or that the file is stored in the location you passed')
        
    df = parse_libpostal(df)
    df = get_s2sG_results(df)
    df = generate_searched_query_on_full_dataframe(df, mapping_dict, subdivision_list)
    
    return df