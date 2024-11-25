"""this function takes two lists of dictionaries in this form:

input_counter_part_data:

{
    "counterparty_id": int,
    "counterparty_legal_name": str,
    "legal_address_id": int,
    "commercial contact": str,
    "delivery_contact": str,
    "created_at": str,
    "updated_at": str
}

and return them in this form:

{
    "counterparty_id": int,
    "counterparty_legal_name": str,
    "counterparty_legal_address_line_1": str,
    "counterparty_legal_address_line_2": str,
    "counterparty_legal_district": str,
    "counterparty_legal_city": str,
    "counterparty_legal_postal_code": str,
    "counterparty_legal_country": str,
    "counterparty_legal_phone_number": str
}

you will need to find the address information from legal_address_id - maybe do some sql on the original database to get the relevant information?"""

try:
    from src.process_data.connection import connect_to_db
    from src.process_data.processing_error import ProcessingError
except ImportError:
    from connection import connect_to_db
    from processing_error import ProcessingError


def get_dim_counterparty(credentials_id, input_counterparty_data):
    print(f"CREDENTIALS {credentials_id}")
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
    try:    
        address = [
            address
            for address in addresses
            if int(address["address_id"]) == int(counterparty["legal_address_id"])
        ][0]

        return {
            "counterparty_id": int(counterparty["counterparty_id"]),
            "counterparty_legal_name": counterparty["counterparty_legal_name"],
            "counterparty_legal_address_line_1": address["address_line_1"],
            "counterparty_legal_address_line_2": address["address_line_2"],
            "counterparty_legal_district": address["district"],
            "counterparty_legal_city": address["city"],
            "counterparty_legal_postal_code": address["postal_code"],
            "counterparty_legal_country": address["country"],
            "counterparty_legal_phone_number": address["phone"],
        }
    except Exception as e:
        raise ProcessingError(f"get_counterparty_dict: {e}")