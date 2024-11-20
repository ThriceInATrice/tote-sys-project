# the data from the currency table comes in as a dictionary
# eg {"currnecy": line1, line2, line3}
# each line is a dict with key/values of column/entry
# eg {
#   currency_id:int,
#   currnecy_code: int,
#   created_at: datetime,
#   last_updated: datetime
# }
# the function should return a list of dictionaries representing the line to be inserted in the final database
# eg {
#   currency_id: int,
#   currency_code: int,
#   currency_name: str
# }
# then return the new list

import json

try:
    from src.process_data.processing_error import ProcessingError
except ImportError:
    from processing_error import processingError


def get_dim_currency(currency_data):

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
    try:
        with open("currencies.json") as currency_json:
            currency_dict = json.load(currency_json)

        return currency_dict[currency_code.lower()]

    except Exception as e:
        raise ProcessingError(f"get_dim_currency: {e}")
