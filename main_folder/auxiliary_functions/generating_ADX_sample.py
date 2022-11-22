# Databricks notebook source
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
import regex
from datetime import date
from pyspark.sql.functions import DataFrame
import azure.kusto.data.exceptions

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Building the functions for ADX:

# COMMAND ----------

def generate_countries_spellings(country_codes: typing.List[str] or str = None,
                                 languages: typing.List[str] = None) -> pd.DataFrame:
    """Generates a DataFrame with spellings in different languages for a country
    :param country_codes: string or list of strings of ISO-2 codes of a country ('ES', not 'ESP'), defaults to None
    :type country_codes: str or list, optional
    :param languages: list of languages to get the spellings from, defaults to None
    :type languages: list, optional
    :return: DataFrame with two columns: country code and spellings
    :rtype: pd.DataFrame
    """
    
    # Get all country keys
    if country_codes is None:
        country_codes=list(dict(countries_for_language('en')).keys())
    
    
    # If country code is not a list, convert to list with one element
    if type(country_codes) != list:
        country_codes = [country_codes]
    
    
    # Define languages to query
    if languages is None:
        languages = [lang for lang in available_languages() if len(lang)==2]
    
    
    # Find country spellings 
    country_spellings = [(country, [dict(countries_for_language(lang))[country.upper()] for lang in languages]) for country in country_codes]
    
    
    # Drop duplicates
    country_spellings = [(country, list(set(spellings))) for country, spellings in country_spellings]
    
    
    # Add ISO codes to list of spellings
    country_spellings = [(country, spellings + [country.upper(), pycountry.countries.get(alpha_2=country.upper()).alpha_3]) 
                        for country, spellings in country_spellings]


    # Add official name if exists
    country_spellings = [(country, spellings + [pycountry.countries.get(alpha_2=country.upper()).official_name])
                         if 'official_name' in str(pycountry.countries.get(alpha_2=country))
                         else (country, spellings)
                         for country, spellings in country_spellings]
    
    
    return pd.DataFrame(country_spellings, columns=['country_code', 'spellings'])

def query_addresses_new(
    country_names: typing.List[str] or typing.Tuple[str] or None,
    populated_fields: typing.List[str] or typing.Tuple[str] or None,
    country: str,
    table: str = 'search_logs_insights',
    endpoint_list: typing.List[str] or typing.Tuple[str] or None = None,
    exclude_endpoint_list: typing.List[str] or typing.Tuple[str] or None = ('search 2 poiSearch'),
    developer_emails_list: typing.List[str] or typing.Tuple[str] or None = ('maps.analytics.metrics@groups.tomtom.com', ''),
    only_full_addresses: bool = True,
    sample: int or None = 100000,
    ago: int or None = 365,
    check_query: bool = False,
    deduplicate: bool = True,
    #filter_results: int = 10000000
) -> str:

    """Function that generates the string query in KQL (kusto query language) to perform in ADX.

    :param country_names: List of names the country can have. Most countries have a lot of denominations and in ADX, the countries denominations are not consistent. So, in order to obtain all possible results, we pass this argument that contain a lot of possible country denominations. This are generated using the "get_country_spellings" function.
    :type country_names: typing.List[str] or typing.Tuple[str] or None
    :param populated_fields: List of populated fields to be selected from the query. Generally, the "only_full_address" parameter is good enough to select all complete addresses, but the populated fields lets you select those responses that have some specific fields included in the response. For example, populated field number "4" is equivalent to house number!
    :type populated_fields: typing.List[str] or typing.Tuple[str] or None
    :param country: Country to be casted to the address column. In order to be consistent with the process it must be the country expresses in its three letter ISO code. So if the country is SPAIN, you should pass 'ESP'.
    :type country: str
    :param table: Table on which you want to perform the query, defaults to 'search_logs_insights'
    :type table: str, optional
    :param endpoint_list: List of the endpoints you want to filter out for your query, defaults to None, which means no endpoint will be filtered out. 
    :type endpoint_list: typing.List[str] or typing.Tuple[str] or None, optional
    :param exclude_endpoint_list: List of endpoints to exclude from the query, in case you don't want them to appear. Defaults to "search 2 poiSearch" since the poi endpoint doesn't provide relevant information for the india process.
    :type exclude_endpoint_list: typing.List[str] or typing.Tuple[str] or str or None
    :param developer_emails_list: List that determines which emails the query should remove from TT developers (like maps analytics). You can pass elements within a list and they will be removed from the search, defaults to ('maps.analytics.metrics@groups.tomtom.com', ''), which means only the maps analytics emails will be exluded.
    :type developer_emails_list: typing.List[str] or typing.Tuple[str] or None, optional
    :param only_full_addresses: Boolean that defines if we are looking only for complete addresses or not. This should be set to True if we only want to get complete results (addresses that haave information up to APT level), defaults to True
    :type only_full_addresses: bool, optional
    :param sample: Sample size you want to extract, defaults to 100000
    :type sample: int, optional
    :param ago: Time (in days) you want the query to include, defaults to 365, which means all queries within 365 days back will be included in the response.
    :type ago: int or None, optional
    :param check_query: Boolean that allows to print the query that was passed to kusto in order to debug. Only switch to True if you are having problems with the query response or the number of responses. Defaults to False, which means that the query shouldn't be printed.
    :type check_query: bool, optional.
    :param deduplicate: Boolean that defines if the addresses returns should be deduplicated or not, defaults to True, which means the addresses will be deduplicated.
    :type deduplicate: bool, optional
    :return: The string to pass to the ADX instance in order to get the response for a specific country.
    :rtype: str
    """

    ###### ERROR Handling ######

    if not ((isinstance(country_names, list)) or (isinstance(country_names, tuple)) or (country_names is not None)):
        input = "country_names"
        raise TypeError('The input "country_names" must be a list, tuple or None')
    else:
        if isinstance(country_names, list):
            country_names = tuple(country_names)

    if not ((isinstance(populated_fields, list)) or (isinstance(populated_fields, tuple)) or (populated_fields is None)):
        input = 'populated_fields'
        raise TypeError(f'The input "{input}" must be a list, tuple or None')
    else:
        if isinstance(populated_fields, list):
            populated_fields = tuple(populated_fields)

    if not ((isinstance(endpoint_list, list)) or (isinstance(endpoint_list, tuple)) or (endpoint_list is None)):
        input = 'endpoint_list'
        raise TypeError(f'The input "{input}" must be a list, tuple or None')
    else:
        if isinstance(endpoint_list, list):
            endpoint_list = tuple(endpoint_list)
    
    if not (
        (isinstance(developer_emails_list, list)) or (isinstance(developer_emails_list, tuple)) or (developer_emails_list is None)
        or (isinstance(developer_emails_list, str))
    ):
        input = 'developer_emails_list'
        raise TypeError(f'The input "{input}" must be a list, tuple, string or None')
    else:
        if isinstance(developer_emails_list, list):
            developer_emails_list = tuple(developer_emails_list)
            
    if not (
        (isinstance(exclude_endpoint_list, list)) or (isinstance(exclude_endpoint_list, tuple)) or (exclude_endpoint_list is None)
        or (isinstance(exclude_endpoint_list, str))
    ):
        input = 'exclude_endpoint_list'
        raise TypeError(f'The input "{input}" must be a list, tuple, string or None')
    else:
        if isinstance(developer_emails_list, list):
            developer_emails_list = tuple(developer_emails_list)
            
    if not ((isinstance(sample, int)) or (sample is None)):
        input = 'sample'
        raise TypeError(f'The input "{input}" must be an int or None')

    ############################
    
    # Turn country_names and populated fields into strings
    country_names = str(tuple(country_names))
    
    ## TODO: Convertir todo lo de abajo en una función externa!! ##
    if ago is not None:
        look_back = f'| where client_received_start_timestamp > ago({int(ago)}d)'
    else:
        look_back = ''
        
    if only_full_addresses:
        who_searched = "| where who_searched == 'Full Address Search'"
    else:
        who_searched = ''
        
    if developer_emails_list is None:
        developer_line = ''
    elif type(developer_emails_list) == str:
        developer_line = f'''| where developer_email !in~ ('{developer_emails_list}')'''
    else:
        developer_line = f'| where developer_email !in~ {str(developer_emails_list)}'

    if exclude_endpoint_list is None:
        developer_line = ''
    elif type(exclude_endpoint_list) == str:
        exclude_developer_line = f'''| where method_name !in~ ('{exclude_endpoint_list}')'''
    else:
        exclude_developer_line = f'| where method_name !in~ {str(exclude_endpoint_list)}'

    if populated_fields is not None:
        populated_fields_string = f"| where populated_fields in {populated_fields}"
    else:
        populated_fields_string = ''

    if endpoint_list is not None:
        endpoint_string = f'| where method_name in~ {endpoint_list}'
    else:
        endpoint_string = ''
        
    if sample is not None:
        sample_string = f'| sample {sample}'
    else:
        sample_string = ''
        
    order_string = ''
        
    if deduplicate:
        deduplicate_string = '| summarize search_query_counts = count() by request_uri, searched_query, populated_fields, ordered_populated_fields, countryName, who_searched, request_country, method_name, lib_postal_result, parsed_request_quertystring, developer_email, house, near, house_number, road, unit, level, entrance, staircase, po_box, postcode, suburb, city_district, city, island, state_district, state, country_region, world_region'
        sample_string = f'| limit {sample}'
        order_string = '| order by search_query_counts'
    else:
        deduplicate_string = ''
        
    # if filter_results is not None:
    #     reduce_sample = f'| limit {filter_results}'
    # else:
    #     reduce_sample = ''
        
        
    ###############################################################

    building_string = f'''{table}
                            {look_back}
                            {who_searched}
                            | where countryName in~ {country_names}
                            {endpoint_string}
                            {exclude_developer_line}
                            {populated_fields_string}
                            {developer_line}
                            {deduplicate_string}
                            {order_string}
                            {sample_string}
                            | extend country = '{country}'
                        '''
    if check_query:
        print('THIS IS THE QUERY YOU EXECUTED ON ADX:')
        print(building_string)
        print('\n')
    
    return building_string

def query_addresses_date_range(
    country_names: typing.List[str] or typing.Tuple[str] or None,
    populated_fields: typing.List[str] or typing.Tuple[str] or None,
    country: str,
    table: str = 'search_logs_insights',
    endpoint_list: typing.List[str] or typing.Tuple[str] or None = None,
    exclude_endpoint_list: typing.List[str] or typing.Tuple[str] or None = ('search 2 poiSearch'),
    developer_emails_list: typing.List[str] or typing.Tuple[str] or None = ('maps.analytics.metrics@groups.tomtom.com', ''),
    only_full_addresses: bool = True,
    sample: int or None = 100000,
    daterange_recent: int or None = 15,
    daterange_furthest: int or None = 30,
    check_query: bool = False,
    deduplicate: bool = True,
) -> str:
    """_summary_

    :param country_names: _description_
    :type country_names: typing.List[str]ortyping.Tuple[str]orNone
    :param populated_fields: _description_
    :type populated_fields: typing.List[str]ortyping.Tuple[str]orNone
    :param country: _description_
    :type country: str
    :param table: _description_, defaults to 'search_logs_insights'
    :type table: str, optional
    :param endpoint_list: _description_, defaults to None
    :type endpoint_list: typing.List[str]ortyping.Tuple[str]orNone, optional
    :param exclude_endpoint_list: _description_, defaults to ('search 2 poiSearch')
    :type exclude_endpoint_list: typing.List[str]ortyping.Tuple[str]orNone, optional
    :param developer_emails_list: _description_, defaults to ('maps.analytics.metrics@groups.tomtom.com', '')
    :type developer_emails_list: typing.List[str]ortyping.Tuple[str]orNone, optional
    :param only_full_addresses: _description_, defaults to True
    :type only_full_addresses: bool, optional
    :param sample: _description_, defaults to 100000
    :type sample: intorNone, optional
    :param daterange_recent: _description_, defaults to 15
    :type daterange_recent: intorNone, optional
    :param daterange_furthest: _description_, defaults to 30
    :type daterange_furthest: intorNone, optional
    :param check_query: _description_, defaults to False
    :type check_query: bool, optional
    :param deduplicate: _description_, defaults to True
    :type deduplicate: bool, optional
    :return: _description_
    :rtype: str
    """
    ###### ERROR Handling ######

    if not ((isinstance(country_names, list)) or (isinstance(country_names, tuple)) or (country_names is not None)):
        input = "country_names"
        raise TypeError('The input "country_names" must be a list, tuple or None')
    else:
        if isinstance(country_names, list):
            country_names = tuple(country_names)

    if not ((isinstance(populated_fields, list)) or (isinstance(populated_fields, tuple)) or (populated_fields is None)):
        input = 'populated_fields'
        raise TypeError(f'The input "{input}" must be a list, tuple or None')
    else:
        if isinstance(populated_fields, list):
            populated_fields = tuple(populated_fields)

    if not ((isinstance(endpoint_list, list)) or (isinstance(endpoint_list, tuple)) or (endpoint_list is None)):
        input = 'endpoint_list'
        raise TypeError(f'The input "{input}" must be a list, tuple or None')
    else:
        if isinstance(endpoint_list, list):
            endpoint_list = tuple(endpoint_list)
    
    if not (
        (isinstance(developer_emails_list, list)) or (isinstance(developer_emails_list, tuple)) or (developer_emails_list is None)
        or (isinstance(developer_emails_list, str))
    ):
        input = 'developer_emails_list'
        raise TypeError(f'The input "{input}" must be a list, tuple, string or None')
    else:
        if isinstance(developer_emails_list, list):
            developer_emails_list = tuple(developer_emails_list)
            
    if not (
        (isinstance(exclude_endpoint_list, list)) or (isinstance(exclude_endpoint_list, tuple)) or (exclude_endpoint_list is None)
        or (isinstance(exclude_endpoint_list, str))
    ):
        input = 'exclude_endpoint_list'
        raise TypeError(f'The input "{input}" must be a list, tuple, string or None')
    else:
        if isinstance(developer_emails_list, list):
            developer_emails_list = tuple(developer_emails_list)
            
    if not ((isinstance(sample, int)) or (sample is None)):
        input = 'sample'
        raise TypeError(f'The input "{input}" must be an int or None')

    ############################
    
    # Turn country_names and populated fields into strings
    country_names = str(tuple(country_names))
        
    if only_full_addresses:
        who_searched = "| where who_searched == 'Full Address Search'"
    else:
        who_searched = ''
        
    if developer_emails_list is None:
        developer_line = ''
    elif type(developer_emails_list) == str:
        developer_line = f'''| where developer_email !in~ ('{developer_emails_list}')'''
    else:
        developer_line = f'| where developer_email !in~ {str(developer_emails_list)}'

    if exclude_endpoint_list is None:
        developer_line = ''
    elif type(exclude_endpoint_list) == str:
        exclude_developer_line = f'''| where method_name !in~ ('{exclude_endpoint_list}')'''
    else:
        exclude_developer_line = f'| where method_name !in~ {str(exclude_endpoint_list)}'

    if populated_fields is not None:
        populated_fields_string = f"| where populated_fields in {populated_fields}"
    else:
        populated_fields_string = ''

    if endpoint_list is not None:
        endpoint_string = f'| where method_name in~ {endpoint_list}'
    else:
        endpoint_string = ''
        
    if sample is not None:
        sample_string = f'| sample {sample}'
    else:
        sample_string = ''
        
    order_string = ''
        
    if deduplicate:
        deduplicate_string = '| summarize search_query_counts = count() by request_uri, searched_query, populated_fields, ordered_populated_fields, countryName, who_searched, request_country, method_name, lib_postal_result, parsed_request_quertystring, developer_email, house, near, house_number, road, unit, level, entrance, staircase, po_box, postcode, suburb, city_district, city, island, state_district, state, country_region, world_region'
        sample_string = f'| limit {sample}'
        order_string = '| order by search_query_counts'
    else:
        deduplicate_string = ''

    look_back = f'| where (ago({daterange_furthest}d) > client_received_start_timestamp) and (client_received_start_timestamp < ago({daterange_recent}d))'
        
    # if filter_results is not None:
    #     reduce_sample = f'| limit {filter_results}'
    # else:
    #     reduce_sample = ''
        
        
    ###############################################################

    building_string = f'''{table}
                            {look_back}
                            {who_searched}
                            | where countryName in~ {country_names}
                            {endpoint_string}
                            {exclude_developer_line}
                            {populated_fields_string}
                            {developer_line}
                            {deduplicate_string}
                            {order_string}
                            {sample_string}
                            | extend country = '{country}'
                        '''
    if check_query:
        print('THIS IS THE QUERY YOU EXECUTED ON ADX:')
        print(building_string)
        print('\n')
    
    return building_string


def parse_populated_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Translate the keys from populated fields into readable content
    :param df: DataFrame of search logs containing column 'populated_fields' for which the components we wish to translate
    :type df: pd.DataFrame
    :return: DataFrame with column 'components' which has the 'populated_fields' components in a readable form.
    :rtype: pd.DataFrame
    """
    
    ## Create connection to ADX and get populated fields master data
    #connections_utils_instance = connections_utils.AzureConnections()
    tenant_id, client_id, secret_value, secret_id = connections_utils.get_adx_secrets()
    adx_instance = adx.AzureDataExplorer()
    populated_fields_master_df, _ = adx_instance.execute_adx_query(query='populated_fields_master_data',
                                                                   cluster="https://ttapianalyticsadxpweu.westeurope.kusto.windows.net",
                                                                   database="ttapianalytics-onlineSearch",
                                                                   client_id=client_id,
                                                                   secret_id=secret_id,
                                                                   tenant_id=tenant_id) 
    
    adx_instance = None
    
    # Parse
    df_copy = df.copy()
    
    # Create dictionary from 'populated_fields_master_df' to help translate populated fields
    fields_dictionary = pd.Series(populated_fields_master_df.populated_field_description.values,
                                  index=populated_fields_master_df.populated_field_id.astype(str)).to_dict()


    # Split by pipes and substitute each number with their corresponding component
    df_copy['components'] = df_copy.populated_fields.str.split('|')

    df_copy['components'] = (df_copy
                             .components
                             .apply(lambda x: [fields_dictionary[comp] for comp in x if comp!=''])
                                     )

    df_copy['number_components'] = df_copy.components.apply(lambda x: len(x))
    
    
    # Transform back to a single string divided by pipes
    df_copy['components'] = df_copy.components.apply(lambda x: '|'.join(x))
    
    return df_copy

def get_country_logs(
    country: str, endpoint_list: list or tuple or None = None, table: str = 'search_logs_insights', sample: int=10000, 
    populated_fields: list or tuple or None = None, developer_emails_list: list or tuple or None = None, ago: int = 365, 
    exclude_endpoint_list: list or tuple or str or None = ('search 2 poiSearch'), only_full_addresses: bool = True, 
    check_query: bool = False, deduplicate: bool = True #, filter_results: int = 10000000
) -> pd.DataFrame:
    """Gets search logs for a given country
    :param country: string of ISO-2 code of a country ('ES', not 'ESP').
    :type country: str
    :param endpoint_list: List of the endpoints you want to filter out for your query, defaults to None, which means no endpoint will be filtered out. 
    :type endpoint_list: typing.List[str] or typing.Tuple[str] or None, optional
    :param table: wether to query 'search_log_insights_new' instead of 'search_logs_insights', defaults to True
    :type table: str, optional
    :param sample: Initial sample size, defaults to 10000
    :type sample: int, optional
    :param populated_fields: List of populated fields to be selected from the query. Generally, the "only_full_address" parameter is good enough to select all complete addresses, but the populated fields lets you select those responses that have some specific fields included in the response. For example, populated field number "4" is equivalent to house number!
    :type populated_fields: typing.List[str] or typing.Tuple[str] or None
    :param endpoints: Tuple that contains the endpoints you want to search for. This parameter should only be filled if the value of "specify_search_method" is True. Possible values are: search 2 search, search 2 structuredGeocode, search 2 nearbySearch, search 2 geocode, search 2 poiSearch, search 2 categorySearch, etc...
    :type endpoints: tuple(str)
    :param developer_emails_list: List that determines which emails the query should remove from TT developers (like maps analytics). You can pass elements within a list and they will be removed from the search, defaults to ('maps.analytics.metrics@groups.tomtom.com', ''), which means only the maps analytics emails will be exluded. Defaults to None.
    :type developer_emails_list: typing.List[str] or typing.Tuple[str] or None, optional
    :param ago: Include how much time (in days) you want to include for the logs that will be returned. Defaults to 365, which means that logs from up to a year back from the date this is run will be included.
    :type ago: int or None, optional
    :param exclude_endpoint_list: List of endpoints to exclude from the query, in case you don't want them to appear. Defaults to "search 2 poiSearch" since the poi endpoint doesn't provide relevant information for the india process.
    :type exclude_endpoint_list: typing.List[str] or typing.Tuple[str] or str or None
    :param only_full_addresses: Boolean that shows if the search should be done on full addresses only, or if the search should be done on all logs. Defaults to True, which means we are only looking for complete address searches.
    :type only_full_addresses: bool, optional
    :param check_query: Boolean that allows to print the query that was passed to kusto in order to debug. Only switch to True if you are having problems with the query response or the number of responses. Defaults to False, which means that the query shouldn't be printed.
    :type check_query: bool, optional.
    :param deduplicate: Boolean that defines if the addresses returns should be deduplicated or not, defaults to True, which means the addresses will be deduplicated.
    :type deduplicate: bool, optional
    :return: DataFrame with logs for the specified search parameters.
    :rtype: pd.DataFrame
    """

    # Country spellings and produce query
    country_names = generate_countries_spellings(country)
        
    query_country = query_addresses_new(
        country_names=country_names.spellings[0],
        populated_fields=populated_fields,
        country=country,
        table=table, 
        endpoint_list=endpoint_list,
        developer_emails_list=developer_emails_list,
        only_full_addresses=only_full_addresses,
        sample=sample,
        ago=ago,
        check_query=check_query,
        deduplicate=deduplicate,
        exclude_endpoint_list=exclude_endpoint_list,
        #filter_results=filter_results
    )

    tenant_id, client_id, secret_value, secret_id = connections_utils.get_adx_secrets()
    adx_instance = adx.AzureDataExplorer()


    try:
        addresses_df, _ = adx_instance.execute_adx_query(query=query_country,
                                                        cluster="https://ttapianalyticsadxpweu.westeurope.kusto.windows.net",
                                                                database="ttapianalytics-onlineSearch",
                                                                client_id=client_id,
                                                                secret_id=secret_id,
                                                                tenant_id=tenant_id) 
    except azure.kusto.data.exceptions.KustoMultiApiError:
        dates = np.linspace(15, 720, 15)
        num_entries, i = 0, 0
        addresses_df = pd.DataFrame()

        while num_entries < sample:
            query_country = query_addresses_date_range(
                country_names=country_names.spellings[0],
                populated_fields=populated_fields,
                country=country,
                table=table, 
                endpoint_list=endpoint_list,
                developer_emails_list=developer_emails_list,
                only_full_addresses=only_full_addresses,
                sample=sample,
                daterange_furthest=dates[i + 1],
                daterange_recent=dates[i],
                check_query=check_query,
                deduplicate=deduplicate,
                exclude_endpoint_list=exclude_endpoint_list,
            )

            adx_df, _ = adx_instance.execute_adx_query(
                query=query_country,
                cluster="https://ttapianalyticsadxpweu.westeurope.kusto.windows.net",
                database="ttapianalytics-onlineSearch",
                client_id=client_id,
                secret_id=secret_id,
                tenant_id=tenant_id
            ) 

            addresses_df = pd.concat([addresses_df, adx_df])
            num_entries = addresses_df.shape[0]
            
            i += 1


    addresses_df = parse_populated_fields(addresses_df)
    addresses_df.countryName = addresses_df.countryName.str.upper()
    
    
    if 'developer_email' not in addresses_df.columns:
        addresses_df['developer_email'] = ''
    
    
    # Close connections
    adx_instance = None
    connections_utils_instance = None

    addresses_clients_df = addresses_df.loc[~addresses_df.searched_query.str.contains('�')].reset_index(drop=True)

    return addresses_clients_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Building a function for sample generation for multiple countries

# COMMAND ----------

def address_components_sample_generator(
    country_list: list, endpoint_list: list or tuple or None = None, table: str = 'search_logs_insights', sample: int = 10000, 
    developer_emails_list: list or tuple or None = None, populated_fields: list or tuple or None = None, only_full_addresses: bool = True, 
    exclude_endpoint_list: list or tuple or None = ('search 2 poiSearch'), ago: int = 365, check_query: bool = False, 
    deduplicate: bool = True, filter_results: int = 10000000
) -> dict:
    '''
    Function that receives the list of countries you want to get the sample for in ISO-2 code, a list of endpoints you want to include in your serach, the table you want to use in ADX, etc. It returns a dictionary with the responses for each of the countries in the list.
    
    :param country_list: List of countries to use in ISO-2 code! For example, if you want the results for Spain and the United States, you should pass country_list = ['ES', 'US'].
    :type country_list: list
    :param endpoint_list: List of the endpoints you want to filter out for your query, defaults to None, which means no endpoint will be filtered out. 
    :type endpoint_list: typing.List[str] or typing.Tuple[str] or None, optional
    :param table: String that selects which table to get the results from, defaults to 'search_logs_insights' which is the main table where results from queries are stored.
    :type table: str, optional
    :param sample: Specify the sample size you want for each query, defaults to 10000
    :type sample: int, optional
    :param developer_emails_list: List that determines which emails the query should remove from TT developers (like maps analytics). You can pass elements within a list and they will be removed from the search, defaults to ('maps.analytics.metrics@groups.tomtom.com', ''), which means only the maps analytics emails will be exluded. Defaults to None.
    :type developer_emails_list: typing.List[str] or typing.Tuple[str] or None, optional
    :param populated_fields: A list that contains all the populated fields combinations that are valid for a complete address, defaults to None, which means populated_fields are not considered.
    :type populated_fields: list or tuple or None, optional
    :param only_full_addresses: Determine if the query should require only full addresses (at APT level), defaults to True, which means only complete addresses should be returned in the Kusto query.
    :type only_full_addresses: bool, optional
    :param exclude_endpoint_list: List of endpoints to exclude from the query, in case you don't want them to appear. Defaults to "search 2 poiSearch" since the poi endpoint doesn't provide relevant information for the india process.
    :type exclude_endpoint_list: typing.List[str] or typing.Tuple[str] or str or None
    :param ago: Time (in number of days) that should be included starting from today's date and going back "ago" days, defaults to 365, which means the results will only include queries from up to 365 days back.
    :type ago: int, optional
    :param check_query: Boolean that allows to print the query that was passed to kusto in order to debug. Only switch to True if you are having problems with the query response or the number of responses. Defaults to False, which means that the query shouldn't be printed.
    :type check_query: bool, optional.
    :param deduplicate: Boolean that defines if the addresses returns should be deduplicated or not, defaults to True, which means the addresses will be deduplicated.
    :type deduplicate: bool, optional
    :return: Returns a dictionary with the countries as keys and the query response dataframes as values for each country.
    :rtype: dict
    '''
    # Create an empty dictionary where we will store all the samples by country name:
    country_dict = {}
    
    # Iterate through the country list and call the get_country_logs function on each country:
    for country in country_list:
        sample_df = get_country_logs(
            country=country, endpoint_list=endpoint_list, table=table, sample=sample, populated_fields=populated_fields, 
            developer_emails_list=developer_emails_list, ago=ago, only_full_addresses=only_full_addresses, 
            exclude_endpoint_list=exclude_endpoint_list, check_query=check_query, deduplicate=deduplicate, 
            #filter_results=filter_results
        )
        
        country_dict[country] = sample_df
        
    return country_dict

# COMMAND ----------

# MAGIC %md
# MAGIC ### Building the function to convert country names to the two-letter ISO code

# COMMAND ----------

def get_countries_correctly(countries: list or str):
    '''
    Function that receives the name of the countries or country and converts it into ISO-2 to be used by the query in ADX.
    
    :param countries: List of the countries you want to convert to ISO-2
    :type countries: list or str
    :return: A python list containing the names of the countries converter to ISO-2
    :rtype: list
    '''
    if type(countries) != list:
        countries = [countries]
    
    return [country_converter.convert(country, to='ISO2') for country in countries]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Building the function to extract and calculate the parsed components from the tables

# COMMAND ----------

def get_libpostal_condition(component, possible_results):
    '''
    :param possible_results: These are the possible outcomes that this value may have. For example a house number can contain spaces, letters and numbers (think of calle de Goya 23 b --> house number is: "23 b")
    :type possible_results: str
    '''
    libpostal_condition = f'"{component}":"{possible_results}"' + '[,\}]'
    
    return libpostal_condition

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

#### SAVING THE SAMPLE ####
def general_dbfs_save_function_dict_of_countries(
    dictionary: dict, path: str, file_type: str, name_append: str, sep: str or None = ';',
    selected_columns: list = ['request_uri', 'searched_query', 'populated_fields', 'countryName', 'who_searched', 'request_country', 'method_name', 'lib_postal_result', 'parsed_request_quertystring', 'developer_email', 'search_query_counts', 'country', 'components', 'number_components'], 
    ) -> None:
    """Function that saves the results of the dataframe you pass into different files following the structure: '{path}/{name_append}_{country}' and appending the format string at the end depending on which you choose.
    
    :param dictionary: Dictionary that contains the countries used to generate the samples as keys and the DataFrames of the samples as values.
    :type dictionary: dict
    :param path: String of the base path on which you want to save the content. This will be the base on which you build the saving path.
    :type path: str
    :param file_type: File type you want to save the data in. Only currently supported formats are csv and parquet.
    :type file_type: str
    :param name_append: The extension you want to paste after you base path. This will be the name of your file in the folder of the base path, without the extension!!
    :type name_append: str
    :param sep: Separator in case you use the csv format, defaults to ';'
    :type sep: str or None, optional
    :param selected_columns: The columns of the DataFrame from the sample that you want to keep. If you are in doubt keep the default options, defaults to ['request_uri', 'searched_query', 'populated_fields', 'countryName', 'who_searched', 'request_country', 'method_name', 'lib_postal_result', 'parsed_request_quertystring', 'developer_email', 'search_query_counts', 'country', 'components', 'number_components']
    :type selected_columns: list, optional
    """
    for country in dictionary.keys():
        df = dictionary[country][selected_columns]
        file_path = f'{path}/{name_append}_{country}'
        if file_type == 'parquet':
            df.to_parquet(file_path + '.parquet')
        elif file_type == 'csv':
            df.to_csv((file_path + '.csv'), sep=sep, header=True)
        else:
            raise NameError(f'The file_type parameter must be "parquet" or "csv", you passed: {file_type}')
            
        print(f'{country.upper()} is done in {file_path}!!')