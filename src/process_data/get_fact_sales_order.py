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
    "agreed_delivery_location_id": str
}
the final input will have an extra column called sales_record_id which is a serial,
so we will generate it as it is input into the final database"""

try:
    from src.process_data.processing_error import ProcessingError
except ImportError:
    from processing_error import ProcessingError


def get_fact_sales_order(purchess_order_table):
    output_list = []
    
    for diction in purchess_order_table:
        new_purchess_order_dict = {}
        
        for key in diction :
            try: 
                if key in {
                    "design_id", 
                    "staff_id",
                    "counterparty_id",
                    "units_sold",
                    "unit_price",
                    "currency_id",
                    "agreed_delivery_location_id"
                }:
                    new_purchess_order_dict[key]= diction[key]
                
                elif key in ["agreed_delivery_date","agreed_payment_date"]:
                    new_purchess_order_dict[key] = diction[key][0:10]
                
                elif key == 'created_at':
                    new_purchess_order_dict["created_date"]= diction[key][0:10]
                    new_purchess_order_dict["created_time"]= diction[key][11:19]
                
                elif key == 'last_updated':
                    new_purchess_order_dict["last_updated_date"]= diction[key][0:10]
                    new_purchess_order_dict["last_updated_time"]= diction[key][11:19]
            
            except Exception as e: 
                raise ProcessingError
        
        output_list.append(new_purchess_order_dict)

    return output_list