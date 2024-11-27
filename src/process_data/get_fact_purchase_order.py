def get_fact_purchase_order(purchase_order_data):
    """
    this function should accept a list of dictionaries of this form:
    {
        "purchase_order_id": int,
        "created_at": str,
        "last_updated": str,
        "staff_id": int,
        "counterparty_id": int,
        "item_code": str,
        "item_quantity": str,
        "item_unit_price": float,
        "currency_id": int,
        "agreed_delivery_date": str,
        "agreed_payment_date": str,
        "agreed_delivery_location_id": str
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
        "item_unit_price": float,
        "currency_id": int,
        "agreed_delivery_date": str,
        "agreed_payment_date": str,
        "agreed_delivery_location_id": str
    }

    this entry will also have another column called “purchase_order_record_id” which is a serial, so we will generate it as we input it into the data warehouse
    """
    if not isinstance(purchase_order_data, list):
        raise TypeError("Input must be a list")
    elif not all([type(el) == dict for el in purchase_order_data]):
        raise TypeError("Input must be a list of dictionaries")

    return [
        {
            "purchase_order_id": purchase_order["purchase_order_id"],
            "created_date": purchase_order["created_at"][:10].replace("-", ""),
            "created_time": purchase_order["created_at"][11:],
            "last_updated_date": purchase_order["last_updated"][:10].replace("-", ""),
            "last_updated_time": purchase_order["last_updated"][11:],
            "staff_id": purchase_order["staff_id"],
            "counterparty_id": purchase_order["counterparty_id"],
            "item_code": purchase_order["item_code"],
            "item_quantity": purchase_order["item_quantity"],
            "item_unit_price": purchase_order["item_unit_price"],
            "currency_id": purchase_order["currency_id"],
            "agreed_delivery_date": purchase_order["agreed_delivery_date"][:10].replace(
                "-", ""
            ),
            "agreed_payment_date": purchase_order["agreed_payment_date"][:10].replace(
                "-", ""
            ),
            "agreed_delivery_location_id": purchase_order[
                "agreed_delivery_location_id"
            ],
        }
        for purchase_order in purchase_order_data
    ]
