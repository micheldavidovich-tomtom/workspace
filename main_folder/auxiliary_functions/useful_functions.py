import numpy as np
import pandas as pd
import typing
import re

def compose_depth_levels_to_providers(depth_levels_list: list, list_providers: list, suffix: str or None = None) -> list:
    """Function that composes the names of the depth_levels_list with the list of providers (list_providers) in an all-to-all manner.
    
    :param depth_levels_list: List of the allowed depth levels to be used. The depth levels currently used in the Hierarchical Index metric are:     ('order_4', 'order_3', 'order_2', 'order_1', 'postal_code', 'country')
    :type depth_levels_list: list
    :param list_providers: List of provider ids set by the process. BEWARE!! Bing, Google, Here and TomTom Genesis are represented in two letter codes, while OSM is represented with the 'osm' code, and ORBIS with the 'orbis' code.
    :type list_providers: list
    :param suffix: In case you want to add a suffix at the end of the composing process. For example, you can add the string '_corrected' at the end as we do in the functions below. The '_' is not needed, the process automatically adds it for you.
    :type suffix: str, defaults to None
    :return: List of the components matched on an all-to-all basis. So if you pass ['order_4', 'country'] and ['tt', 'orbis'] you will get: ['order_4_tt', 'country_tt', 'order_4_orbis', 'country_orbis'] as the result.
    :rtype: list
    """
    if suffix is None:
        suffix = ''
        
    else:
        suffix = '_' + suffix
        
    exploded_providers_list = [f'{depth_level}_{provider}{suffix}' for provider in list_providers for depth_level in depth_levels_list]
    
    return exploded_providers_list


def bootstrap_resample(df: pd.Series, agg_fun: typing.Callable, times: int = 1000, seed: int = 0) -> list:
    """Function that resamples data with replacement over a dataframe and calculates the aggregate function to be 
    called on the resulting sample.

    :param df: Column of a Pandas DataFrame that contains the sample for which you want to calculate the aggregate 
    function
    :type df: pd.Series
    :param agg_fun: Aggregate function you want to use on the data.
    :type agg_fun: typing.Callable
    :param times: Number of iterations you want on your experiment, defaults to 1000
    :type times: int, optional
    :param seed: Set the initial seed of the experiment. The remaining seeds will be obtained by adding +1 in each 
    iteration, so the final seed will be final_seed = seed + times, defaults to 0
    :type seed: int, optional
    :return: A list that contains the aggregate result for each experiment conducted.
    :rtype: list
    """
    reboot = []
    
    for t in range(times):
        df_boot = df.sample(frac = 1, replace=True, random_state = t+seed)
        reboot.append(agg_fun(df_boot))
        
    return reboot


def percentile_bootstrap(df, agg_fun, conf=0.9, times=1000, seed=0):
    """Generic Percentile Bootstrap
    This function returns a percentile bootstrap confidence interval for a statistic.
    
    :param df: DataFrame with the observed random vectors. Each row represents an observation an each column is a 
    random variable.
    :type df: pd.DataFrame
    :param agg_fun: Aggregate function you want to use on the data. This is the statistic you want to use on your data.
    :param conf: Confidence level of the returned interval. Defaults to 0.9.
    :type conf: float, optional
    :param times: Number of Bootstrap resamples. Defaults to 1000.
    :type times: int, optional
    :param seed: Initial Random seed. Defaults to 0.
    :type seed: int, optional
    :return:
    :rtype: np.array
    Returns:
        numpy.array: Percentile Boostrap CI [lower, upper]
    """    
    reboot = bootstrap_resample(df, agg_fun, times, seed)
    lower_bound, upper_bound = np.quantile(reboot, [(1-conf)/2, (1-conf)/2+conf])
    
    return lower_bound, upper_bound


def add_to_end(base_string, element):
    return base_string + element


class NonMatchingDimensions(Exception):
    """Error used when dimensions of two array-like elements do not match"""
    def __init__(self, message="Two vectors with non-matching dimensions were given!"):
        self.message = message
        super().__init__(self.message)
        
class InputError(Exception):
    """Defines the error that happens when the user provides the wrong input to a function"""
    pass


def destroy_underscores(text_to_destroy: str) -> str:
    """Function that receives text with underscores at the beginning and removes all of them!
    :param text_to_destroy: The text that might contain underscores
    :type text_to_destroy: str
    :return: The same text but with the underscores destroyed
    :rtype: str
    """
    
    return re.sub(r'^_+', '', text_to_destroy)