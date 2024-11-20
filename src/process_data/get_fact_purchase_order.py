def get_fact_purchase_order(purchase_order_data):
    """this function should accept a list of dictionaries of this form:
    {
        "purchase_order_id": int,
        "created_at": str,
        "last_updated": str,
        "staff_id": int,
        "counterparty_id": int,
        "item_code": str,
        "item_quantity": str,
        "item_unit_price": str,
        "currency_id": int,
        "agreed_delivery_date": str,
        "agreed_payment_date": str,
        "agreed_delivery_lcoation_id": str
    }

    and return them in this form:
    {
        "purchase_order_id": int,
        "created_date": str,
        "created_time": str,
        "last_updated_date": str,
        "last_updated_time": str,
        "staff_id": int,
        "counterparty_id": int,
        "item_code": str,
        "item_quantity": str,
        "item_unit_price": str,
        "currency_id": int,
        "agreed_delivery_date": str,
        "agreed_payment_date": str,
        "agreed_delivery_lcoation_id": str
    }

    this entry will also have another column called “purchase_order_record_id” which is a serial, so we will generate it as we input it into the data warehouse"""
    return [{
        "purchase_order_id": purchase_order_data[i]["purchase_order_id"],
        "created_date":	purchase_order_data[i]["created_at"][:4]+purchase_order_data[i]["created_at"][5:7]+purchase_order_data[i]["created_at"][8:10],
        "created_time": purchase_order_data[i]["created_at"][11:],
        "last_updated_date": purchase_order_data[i]["last_updated"][:4]+purchase_order_data[i]["last_updated"][5:7]+purchase_order_data[i]["last_updated"][8:10],
        "last_updated_time":  purchase_order_data[i]["last_updated"][11:],
        "staff_id":	purchase_order_data[i]["staff_id"],
        "counterparty_id":	purchase_order_data[i]["counterparty_id"],
        "item_code": purchase_order_data[i]["item_code"],
        "item_quantity": purchase_order_data[i]["item_quantity"],
        "item_unit_price": purchase_order_data[i]["item_unit_price"],
        "currency_id": purchase_order_data[i]["currency_id"],
        "agreed_delivery_date":	purchase_order_data[i]["agreed_delivery_date"][:4]+purchase_order_data[i]["agreed_delivery_date"][5:7]+purchase_order_data[i]["agreed_delivery_date"][8:10],
        "agreed_payment_date": purchase_order_data[i]["agreed_payment_date"][:4]+purchase_order_data[i]["agreed_payment_date"][5:7]+purchase_order_data[i]["agreed_payment_date"][8:10],
        "agreed_delivery_location_id": purchase_order_data[i]["agreed_delivery_location_id"]
    } for i in range(len(purchase_order_data))]