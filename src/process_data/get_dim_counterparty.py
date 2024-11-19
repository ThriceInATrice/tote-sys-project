'''this function takes two lists of dictionaries in this form:

input_counter_part_data:

{
    "counterparty_id": int,
    "counterparty_legal_name": str,
    "legal_adress_id": int,
    "commercial contact": str,
    "delivery_contact": str,
    "created_at": str,
    "updated_at": str
}

input_address_data:

{
    "address_id": int,
    "address_line_1": str,
    "address_line_2": str,
    "district": str,
    "city": str,
    "postal_code": str,
    "country": str,
    "phone": str,
    "created_at": timestamp,
    "last_updated": timestamp
}

and return them in this form:

{
    "counterparty_id": int,
    "counterparty_legal_name": str,
    "counterparty_legal_adress_line_1": str,
    "counterparty_legal_address_line_2": str,
    "counterparty_legal_district": str,
    "counterparty_legal_city": str,
    "counterparty_legal_postal_code": str,
    "counterparty_legal_country": str,
    "counterparty_legal_phone_number": str
}

you will need to find the address information from legal_address_id - maybe do some sql on the original database to get the relevant information?'''
from pprint import pprint

class ValueNotFoundError(Exception):
    def __init__(self, item):
        print(item)

def get_dim_counterparty(input_counterparty_data, input_address_data):

    def create_dict(counterparty, address):
        output_dict = {
            "counterparty_id": int(counterparty['counterparty_id']),
            "counterparty_legal_name": counterparty['counterparty_legal_name'],
            "counterparty_legal_adress_line_1": address['address_line_1'],
            "counterparty_legal_address_line_2": address['address_line_2'],
            "counterparty_legal_district": address['district'],
            "counterparty_legal_city": address['city'],
            "counterparty_legal_postal_code": address['postal_code'],
            "counterparty_legal_country": address['country'],
            "counterparty_legal_phone_number": address['phone']
            }
        
        return output_dict

    def search_for_address_id(counterparty):
        counter = 0
        legal_address_id = counterparty['legal_address_id']

        for address in input_address_data:
            counter += 1

            if address['address_id'] == legal_address_id:
                new_dict = create_dict(counterparty, address)
                return new_dict

            elif counter > len(input_address_data):
                raise ValueNotFoundError(f'counterparty legal address id: {counterparty["legal_address_id"]} not found in address table')

    output_list = [search_for_address_id(counterparty) for counterparty in input_counterparty_data]
    pprint(output_list)
    return output_list