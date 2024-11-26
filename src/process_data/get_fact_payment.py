def get_fact_payment(payment_data):
    """
    this function should accept a list of dictionaries in this form:
        {
            "payment_id": int,
            "created_at": str,
            "last_updated": str,
            "transaction_id": int,
            "counterparty_id": int,
            "payment_amount": float,
            "currency_id": int,
            "payment_type_id": int,
            "paid": bool,
            "payment_date": str,
            "company_ac_number": int,
            "counterparty_ac_number": int
        }

    and return them in this form:
        {
            "payment_id": int,
            "created_time": str,
            "created_date": str,
            "last_updated_time": str,
            "last_updated_date": str,
            "transaction_id": int,
            "counterparty_id": int,
            "payment_amount": float,
            "currency_id": int,
            "payment_type_id": int,
            "paid": bool,
            "payment_date": str,
        }

    this entry will also have another column called “payment_record_id” which is a serial, so we will generate it as we input it into the data warehouse
    """
    return [
        {
            "payment_id": payment_dict["payment_id"],
            "created_time": payment_dict["created_at"][11:],
            "created_date": payment_dict["created_at"][:10].replace("-", ""),
            "last_updated_time": payment_dict["last_updated"][11:],
            "last_updated_date": payment_dict["last_updated"][:10].replace("-", ""),
            "transaction_id": payment_dict["transaction_id"],
            "counterparty_id": payment_dict["counterparty_id"],
            "payment_amount": payment_dict["payment_amount"],
            "currency_id": payment_dict["currency_id"],
            "payment_type_id": payment_dict["payment_type_id"],
            "paid": payment_dict["paid"],
            "payment_date": payment_dict["payment_date"][:10].replace("-", ""),
        }
        for payment_dict in payment_data
    ]
