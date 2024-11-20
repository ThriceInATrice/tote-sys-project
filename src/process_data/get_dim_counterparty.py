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


test_counterparty_list = [
            {
                "counterparty_id": "1",
                "counterparty_legal_name": "legal name",
                "legal_address_id": "1",
                "commercial_contact": "aaaaa",
                "delivery_contact": "del cont",
                "created_at": "1170-55-82 22: 36: 84.538631",
                "last_updated": "8543-72-06 57: 41: 44.065852"
            },
            {
                "counterparty_id": "2",
                "counterparty_legal_name": "a b and c",
                "legal_address_id": "2",
                "commercial_contact": "asudadu asd",
                "delivery_contact": "dshkdskhadkahs",
                "created_at": "7707-43-34 94: 62: 82.606200",
                "last_updated": "9389-30-37 72: 47: 03.125707"
            },
            {
                "counterparty_id": "3",
                "counterparty_legal_name": "ahhfsah",
                "legal_address_id": "3",
                "commercial_contact": "adfhhfad",
                "delivery_contact": "adfhadfh",
                "created_at": "7338-14-48 77: 07: 86.797952",
                "last_updated": "0644-87-01 51: 79: 17.342596"
            }
        ]




from pprint import pprint
from src.process_data.connection import connect_to_db

class ValueNotFoundError(Exception):
    def __init__(self, item):
        print(item)

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

#     def search_for_address_id(counterparty):
#         # counter = 0
#         legal_address_id = counterparty['legal_address_id']

#         for address in input_address_data:
#             # counter += 1

#             if address['address_id'] == legal_address_id:
#                 new_dict = create_dict(counterparty, address)
#                 return new_dict

#             # elif counter > len(input_address_data):
#         raise ValueNotFoundError(f'counterparty legal address id: {counterparty["legal_address_id"]} not found in address table')

#     output_list = [search_for_address_id(counterparty) for counterparty in input_counterparty_data]
#     pprint(output_list)
#     return output_list




        # "address_id": "1",
        #         "address_line_1": "address1line1",
        #         "address_line_2": "address1line2",
        #         "district": "district1",
        #         "city": "city1",
        #         "postal_code": "82450",
        #         "country": "Suriname",
        #         "phone": "0743 991197",
        #         "created_at": "4088-56-81 39: 16: 74.051536",
        #         "last_updated": "6652-41-43 39: 28: 38.689756"



def get_dim_counterparty(credentials_id, input_counterparty_data):
    query_string = """SELECT address_id, address_line_1, address_line_2, district, city, postal_code, country, phone FROM address LIMIT 10"""
    
    conn = connect_to_db(credentials_id)
    with conn.cursor() as cursor:
        cursor.execute(query_string)
        addresses = cursor.fetchall()

    address_columns = ('address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone')
    address_list = []
    for address in addresses:
        address_list.append(dict(zip(address_columns, address)))

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

        for address in address_list:
            if legal_address_id == address['address_id']:
                new_dim_counterparty_dict = create_dim_counterparty_dict(counterparty, address)
                processed_counterparty_data.append(new_dim_counterparty_dict)


    pprint(processed_counterparty_data)
    return processed_counterparty_data   

if __name__ == '__main__':
    get_dim_counterparty(None, test_counterparty_list)