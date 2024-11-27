from src.process_data.get_fact_payment import get_fact_payment


def test_get_fact_payment_returns_correct_list_for_1_dict():
    input_list = [
        {
            "payment_id": 1,
            "created_at": "1379-12-08 36: 57: 26.277987",
            "last_updated": "1592-12-08 06: 64: 13.908652",
            "transaction_id": 1,
            "counterparty_id": 1,
            "payment_amount": 099396.16,
            "currency_id": 1,
            "payment_type_id": 1,
            "paid": False,
            "payment_date": "2296-06-30",
            "company_ac_number": 97608040,
            "counterparty_ac_number": 25360783,
        }
    ]

    expected_list = [
        {
            "payment_id": 1,
            "created_time": "36: 57: 26.277987",
            "created_date": "13791208",
            "last_updated_time": "06: 64: 13.908652",
            "last_updated_date": "15921208",
            "transaction_id": 1,
            "counterparty_id": 1,
            "payment_amount": 099396.16,
            "currency_id": 1,
            "payment_type_id": 1,
            "paid": False,
            "payment_date": "22960630",
        }
    ]

    result = get_fact_payment(input_list)
    assert result == expected_list


def test_get_fact_payment_returns_correct_list_for_multiple_dicts():
    input_list = [
        {
            "payment_id": 1,
            "created_at": "1379-09-08 06: 57: 26.277987",
            "last_updated": "1592-03-08 06: 54: 13.908652",
            "transaction_id": 1,
            "counterparty_id": 1,
            "payment_amount": 099396.16,
            "currency_id": 1,
            "payment_type_id": 1,
            "paid": False,
            "payment_date": "2296-06-30",
            "company_ac_number": 97608040,
            "counterparty_ac_number": 25360783,
        },
        {
            "payment_id": 2,
            "created_at": "5945-09-15 59: 32: 55.698339",
            "last_updated": "6653-04-11 57: 22: 14.914783",
            "transaction_id": 2,
            "counterparty_id": 2,
            "payment_amount": 719904.90,
            "currency_id": 2,
            "payment_type_id": 2,
            "paid": True,
            "payment_date": "3434-05-15",
            "company_ac_number": 40476629,
            "counterparty_ac_number": 18337129,
        },
    ]

    expected_list = [
        {
            "payment_id": 1,
            "created_time": "06: 57: 26.277987",
            "created_date": "13790908",
            "last_updated_time": "06: 54: 13.908652",
            "last_updated_date": "15920308",
            "transaction_id": 1,
            "counterparty_id": 1,
            "payment_amount": 099396.16,
            "currency_id": 1,
            "payment_type_id": 1,
            "paid": False,
            "payment_date": "22960630",
        },
        {
            "payment_id": 2,
            "created_time": "59: 32: 55.698339",
            "created_date": "59450915",
            "last_updated_time": "57: 22: 14.914783",
            "last_updated_date": "66530411",
            "transaction_id": 2,
            "counterparty_id": 2,
            "payment_amount": 719904.90,
            "currency_id": 2,
            "payment_type_id": 2,
            "paid": True,
            "payment_date": "34340515",
        },
    ]

    result = get_fact_payment(input_list)
    assert result == expected_list
