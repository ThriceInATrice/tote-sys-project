def process_payment_type(payment_type_table):
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
    print(output_list)
    return output_list
    