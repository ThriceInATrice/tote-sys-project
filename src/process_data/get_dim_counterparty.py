try:
    from src.process_data.connection import connect_to_db
    from src.process_data.processing_error import ProcessingError
except ImportError:
    from connection import connect_to_db
    from processing_error import ProcessingError


def escape_quotes(input_str):
    if "'" in input_str:
        return input_str.replace("'", "")
    else:
        return input_str

def get_dim_counterparty(credentials_id, input_counterparty_data):
    """
    this function takes a list of dictionaries representing rows in the origin datebase
    and transforms them to the appropriate form for the data warehouse
    to do so it need to connection to the origin database to find address data
    then it calls get_counterparty_dict to transform each dictionary
    """

    try:
        conn = connect_to_db(credentials_id)
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM address;")
            response = cursor.fetchall()
    
        column_names = [desc[0] for desc in cursor.description]
        addresses = [dict(zip(column_names, address)) for address in response]
        return [
            get_counterparty_dict(counterparty, addresses)
            for counterparty in input_counterparty_data
        ]
    except Exception as e:
        raise ProcessingError(f"get_dim_counterparty: {e}")
        


def get_counterparty_dict(counterparty, addresses):
    """
    this function transforms a dictionary representing a line from the counterparty table in the origin database
    into a dictionary representing the same data in the data warehouse
    """
    address = [
        address
        for address in addresses
        if int(address["address_id"]) == int(counterparty["legal_address_id"])
    ][0]

    return {
        "counterparty_id": int(counterparty["counterparty_id"]),
        "counterparty_legal_name": escape_quotes(counterparty["counterparty_legal_name"]),
        "counterparty_legal_address_line_1": address["address_line_1"],
        "counterparty_legal_address_line_2": address["address_line_2"],
        "counterparty_legal_district": address["district"],
        "counterparty_legal_city": address["city"],
        "counterparty_legal_postal_code": address["postal_code"],
        "counterparty_legal_country": address["country"],
        "counterparty_legal_phone_number": address["phone"],
    }
