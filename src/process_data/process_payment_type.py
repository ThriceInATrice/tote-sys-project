'''this should take a list of dictionaries of this form:

{
    "payment_type_id": int,
    "payment_type": str,
    "created_at": str,
    "updated_at": str
}

and return them in this form:

{
    "payment_type_id": int,
    "payment_type": str,
}'''

def process_payment_type(payment_type_table):

    def create_dict(item):

        try:
            new_payment_type_dict = {
                'payment_type_id': item['payment_type_id'], 
                'payment_type_name': item['payment_type_name']}
            
            return new_payment_type_dict
        
        except:
            pass

    output_list = [create_dict(item) for item in payment_type_table]
    return output_list