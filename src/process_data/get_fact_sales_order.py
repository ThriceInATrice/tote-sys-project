"""this function should take a list of dictionaries of this form:

{
    "sales_order_id": int,
    "created_at": str,
    "last_updated": str,
    "design_id": int, 
    "staff_id": int,
    "counterparty_id": int,
    "units_sold": int,
    "unit_price": int,
    "currency_id": int,
    "agreed_delivery_date": str,
    "agreed_payment_date": str,
    "agreed_delivery_lcoation_id": str
}
and return them in this form:

{
    "sales_order_id": int,
    "created_date": str, #split
    "created_time": str, #split
    "last_updated_date": str, #split
    "last_updated_time": str, #split
    "design_id": int,
    "staff_id": int,
    "counterparty_id": int,
    "units_sold": int,
    "unit_price": int,
    "currency_id": int,
    "agreed_delivery_date": str,
    "agreed_payment_date": str,
    "agreed_delivery_lcoation_id": str
}
the final input will have an extra column called sales_record_id which is a serial,
so we will generate it as it is input into the final database"""

def process_sales_order(purchess_order_table):
    output_list = []
    for diction in purchess_order_table:
        new_purchess_order_dict = {}
        for key in diction :
            try: 
                if key in {"design_id", 
                            "staff_id",
                            "counterparty_id",
                            "units_sold",
                            "unit_price",
                            "currency_id",
                            "agreed_delivery_date",
                            "agreed_payment_date",
                            "agreed_delivery_lcoation_id"}:
                    new_purchess_order_dict[key]= diction[key]
                elif key == 'created_at':
                    new_purchess_order_dict["created_date"]= diction[key].split()[0]
                    new_purchess_order_dict["created_time"]= diction[key].split()[1]
                elif key == 'last_updated':
                    new_purchess_order_dict["last_updated_date"]= diction[key].split()[0]
                    new_purchess_order_dict["last_updated_time"]= diction[key].split()[1]
            except: raise Exception
        output_list.append(new_purchess_order_dict)
    print(output_list)
    return output_list
    