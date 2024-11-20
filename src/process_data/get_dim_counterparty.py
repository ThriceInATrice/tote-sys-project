'''this function takes two lists of dictionaries in this form:

input_counterparty_data:

{
    "counterparty_id": int,
    "counterparty_legal_name": str,
    "legal_adress_id": int,
    "commercial contact": str,
    "delivery_contact": str,
    "created_at": str,
    "updated_at": str
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
from src.process_data.connection2 import query_database
from src.process_data.get_table_from_full_db import get_table_from_full_db

# class ValueNotFoundError(Exception):
#     def __init__(self, item):
#         print(item)

# def get_dim_counterparty(input_counterparty_data, input_address_data):

#     def create_dict(counterparty, address):
#         output_dict = {
#             "counterparty_id": int(counterparty['counterparty_id']),
#             "counterparty_legal_name": counterparty['counterparty_legal_name'],
#             "counterparty_legal_address_line_1": address['address_line_1'],
#             "counterparty_legal_address_line_2": address['address_line_2'],
#             "counterparty_legal_district": address['district'],
#             "counterparty_legal_city": address['city'],
#             "counterparty_legal_postal_code": address['postal_code'],
#             "counterparty_legal_country": address['country'],
#             "counterparty_legal_phone_number": address['phone']
#             }
        
#         return output_dict

#     def search_for_address_id_in_new_data(counterparty):
#         legal_address_id = counterparty['legal_address_id']
        
#         for address in input_address_data:
#             if address['address_id'] == legal_address_id:
#                 new_dict = create_dict(counterparty, address)
#                 return new_dict
            
#     def search_for_address_id_in_old_data(counterparty):
#         legal_address_id = (counterparty['legal_address_id'])

#         for address in get_table_from_full_db('address'):
#             if address['address_id'] == legal_address_id:
#                 new_dict = create_dict(counterparty, address)
#                 return new_dict

#     def search_for_address_id(counterparty):
#         address_in_new_data = search_for_address_id_in_new_data(counterparty)
#         if address_in_new_data is not None:
#             return address_in_new_data
        
#         address_in_old_data = search_for_address_id_in_old_data(counterparty)
#         if address_in_old_data is not None:
#             return address_in_old_data


#     output_list = [search_for_address_id(counterparty) for counterparty in input_counterparty_data]
#     return output_list



def get_dim_counterparty(credentials_id, input_counterparty_data):

    query_string = """SELECT address_id, address_line_1, address_line_2, district, city, postal_code, country, phone FROM address LIMIT 10"""
    addresses = query_database(credentials_id, query_string)

    address_columns = ('address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone')
    address_dict_list = [dict(zip(address_columns, address)) for address in addresses]


    def create_dim_counterparty_dict(counterparty, address):
        dim_counterparty_dict =  {
        "counterparty_id": counterparty['counterparty_id'],
        "counterparty_legal_name": counterparty['counterparty_legal_name'],
        "counterparty_legal_address_line_1": address['address_line_1'],
        "counterparty_legal_address_line_2": address['address_line_2'],
        "counterparty_legal_district": address['district'],
        "counterparty_legal_city": address['city'],
        "counterparty_legal_postal_code": address['postal_code'],
        "counterparty_legal_country": address['country'],
        "counterparty_legal_phone_number": address['phone']
        }

        return dim_counterparty_dict

    processed_counterparty_data = []

    
    for counterparty in input_counterparty_data:
        legal_address_id = int(counterparty['legal_address_id'])

        for address in address_dict_list:
            if legal_address_id == address['address_id']:
                new_dim_counterparty_dict = create_dim_counterparty_dict(counterparty, address)
                processed_counterparty_data.append(new_dim_counterparty_dict)


    # pprint(processed_counterparty_data)
    return processed_counterparty_data   

# if __name__ == '__main__':
#     get_dim_counterparty(None, test_counterparty_list)