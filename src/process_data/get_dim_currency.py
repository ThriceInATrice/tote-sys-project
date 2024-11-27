import json

try:
    from src.process_data.processing_error import ProcessingError
except ImportError:
    from processing_error import ProcessingError


def get_dim_currency(currency_data):
    """
    this function takes a list of dictionaries representing rows in the origin datebase
    and transforms them to the appropriate form for the data warehouse
    to do so it references a json of currency names and codes using get_currency_name
    """
    return [
        {
            "currency_id": entry["currency_id"],
            "currency_code": entry["currency_code"],
            "currency_name": get_currency_name(entry["currency_code"]),
        }
        for entry in currency_data
    ]


# func to use a currency code to get a currency name from a currency api
def get_currency_name(currency_code):
    """
    this function takes a currency code as argument and returns the corresponding currency name
    by referencing a json of currnecy codes and names
    """
    try:
        with open("currencies.json") as currency_json:
            currency_dict = json.load(currency_json)

        return currency_dict[currency_code.lower()]

    except Exception as e:
        raise ProcessingError(f"get_dim_currency: {e}")
