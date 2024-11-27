def get_dim_payment_type(payment_type_data):
    """
    this should take a list of dictionaries of this form:
        {
            "payment_type_id": int,
            "payment_type_name": str,
            "created_at": str,
            "updated_at": str
        }

    and return them in this form:
        {
            "payment_type_id": int,
            "payment_type": str,
        }
    """

    return [
        {
            "payment_type_id": int(payment_type_dict["payment_type_id"]),
            "payment_type_name": payment_type_dict["payment_type_name"],
        }
        for payment_type_dict in payment_type_data
    ]
