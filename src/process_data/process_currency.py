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


def process_currency(currency_data):
    processed_data = {
        "currency_id": currency_data["currency_id"],
        "currency_code": currency_data["currency_code"],
        "currency_name": get_currency_name(currency_data["currency_code"]),
    }

    return processed_data


# func to use a currency code to get a currency name from a currency api
def get_currency_name(currency_code):
    pass
