def api_calls_parser(response: dict, id_num: int, searched_query: str, threshold: float = 0.85) -> dict:
    """Function that receives a response from the API and returns a dictionary with the parsed components used in the process.
    
    :param response: Dictionary with the TT response to a search query
    :type response: dict
    :param id_num: ID number used to identify this search query for a particular run.
    :type id_num: int
    :param searched_query: The original searched query. The API formats the query, so if you want the original one, it has to be passed directly to this function.
    :type searched_query: str
    :param threshold: Establish the threshold for which an address is considered a match. If the API confidence is higher than the threshold, you will get a 'match' key with a value equal to one in the response dictionary. If the confidence is lower you will get a 'match' key with a value of zero. Defaults to 0.85 and must be between 0 and 1
    :type threshold: float
    :return: Dictionary that parses all the elements needed in the process.
    :rtype: dict
    """
    if ((threshold > 1) or (threshold < 0)):
        raise ValueError(f'The value for the threshold parameter should be between 0 and 1! You passed: {threshold}')
    
    try:
        first_result = response.get("results", None)[0]

        original_formatted_query = response.get('summary', None)['query']
        confidence = first_result['matchConfidence']['score']
        lat = first_result['position']['lat']
        lon = first_result['position']['lon']
        query_type = first_result['type']
        bounding_box = first_result.get('boundingBox', None)
        if confidence > threshold:
            passes = 1
        else:
            passes = 0

        return {
            'original_formatted_query': original_formatted_query, 'confidence': confidence, 'lat': lat, 'lon': lon, 'query_type': query_type, 
            'id': id_num, 'bounding_box': str(bounding_box), 'searched_query': searched_query, 'match': passes
        }
    except IndexError:
        return {
            'original_formatted_query': None, 'confidence': -1, 'lat': None, 'lon': None, 'query_type': None, 
            'id': id_num, 'bounding_box': None, 'searched_query': searched_query, 'match': -1
        }