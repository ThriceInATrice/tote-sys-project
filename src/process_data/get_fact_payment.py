def get_fact_payment(payment_data):
    """this function should accept a list of dictionaries in this form:
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

    when input into the final database there will be an additional column called “payment_record_id” which is a serial,
    so we will generate this as it is input into the data warehouse in the next step"""
    return [{
        "payment_id": payment_data[i]["payment_id"],
        "created_time": payment_data[i]["created_at"][11:],
        "created_date": payment_data[i]["created_at"][:10],
        "last_updated_time": payment_data[i]["last_updated"][11:],
        "last_updated_date": payment_data[i]["last_updated"][:10],
        "transaction_id": payment_data[i]["transaction_id"],
        "counterparty_id": payment_data[i]["counterparty_id"],
        "payment_amount": payment_data[i]["payment_amount"],
        "currency_id": payment_data[i]["currency_id"],
        "payment_type_id": payment_data[i]["payment_type_id"],
        "paid": payment_data[i]["paid"],
        "payment_date": payment_data[i]["payment_date"]
    } for i in range(len(payment_data))]