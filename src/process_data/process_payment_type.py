'''takes the payment_type_table and returns a list of dictionaries.
the dicts are in the format of:

{payment_type_id: str, payment_type_name: str}'''


def process_payment_type(payment_type_table):

    # def create_dict(item):
    #     new_payment_type_dict = {'payment_type_id': item['payment_type_id'], 'payment_type_name': item['payment_type_name']}
    #     return new_payment_type_dict

    # output_list = [create_dict(item) for item in payment_type_table]

    output_list = []
    for item in payment_type_table:

        new_payment_type_dict = {}
        try:
            new_payment_type_dict['payment_type_id'] = item['payment_type_id']
        except:
            raise Exception
        try:
            new_payment_type_dict['payment_type_name'] = item['payment_type_name']
        except:
            raise Exception
        output_list.append(new_payment_type_dict)

    return output_list
    