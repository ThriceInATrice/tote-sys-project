""" 
counterparty data arrives as a list of dictionaries in this form, 
each representing a line in the dim_counterparty table:
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

this function takes this list and returns a single string representing 
an sql command to insert this data into the dim_counterparty table
"""

from src.load.connection import connect_to_db

def get_dim_counterparty_insert_query(counterparty_data):
    
    value_list = []

    for counterparty_dict in counterparty_data:
        value_list.append(
             f"({counterparty_dict["counterparty_id"]}, 
                {counterparty_dict["counterparty_legal_name"]}, 
                {counterparty_dict["counterparty_legal_adress_line_1"]}, 
                {counterparty_dict["counterparty_legal_address_line_2"]}, 
                {counterparty_dict["counterparty_legal_district"]}, 
                {counterparty_dict["counterparty_legal_city"]}, 
                {counterparty_dict["counterparty_legal_postal_code"]}, 
                {counterparty_dict["counterparty_legal_country"]}, 
                {counterparty_dict["counterparty_legal_phone_number"]}"
        )
    
    insert_query = f"""
    INSERT INTO dim_counterparty (
        counterparty_id, 
        counterparty_legal_name, 
        counterparty_legal_adress_line_1, 
        counterparty_legal_address_line_2,
        counterparty_legal_district,
        counterparty_legal_city,
        counterparty_legal_postal_code,
        counterparty_legal_country,
        counterparty_legal_phone_number
    )
    VALUES 
    {",".join(value_list)}
    RETURNING *
    ;
    """
    
    return insert_query

def insert_dim_counterparty(credentials_id, counterparty_data):

    insert_query =  get_dim_counterparty_insert_query(counterparty_data)

    conn = connect_to_db(credentials_id)
    with conn.cursor() as cursor:
        cursor.execute(insert_query)
        response = cursor.fetchall()
    
    return response