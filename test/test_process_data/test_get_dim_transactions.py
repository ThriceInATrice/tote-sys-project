from src.process_data.get_dim_transaction import get_dim_transaction


def test_process_staff_returns_correct_data_for_single_dict():
    input_list = [
        {
            "transaction_id": 1,
            "transaction_type": "PURCHASE",
            "sales_order_id": None,
            "purchase_order_id": 2,
            "created_at": "2022-11-03 14:20:52.186000",
            "updated_at": "2022-11-03 14:20:52.186000",
        }
    ]
    expected_list = [
        {
            "transaction_id": 1,
            "transaction_type": "PURCHASE",
            "sales_order_id": None,
            "purchase_order_id": 2,
        }
    ]
    result = get_dim_transaction(input_list)
    assert result == expected_list


def test_process_staff_returns_correct_data_for_2_dicts():
    input_list = [
        {
            "transaction_id": 1,
            "transaction_type": "PURCHASE",
            "sales_order_id": None,
            "purchase_order_id": 2,
            "created_at": "2022-11-03 14:20:52.186000",
            "updated_at": "2022-11-03 14:20:52.186000",
        },
        {
            "transaction_id": 2,
            "transaction_type": "PURCHASE",
            "sales_order_id": None,
            "purchase_order_id": 5,
            "created_at": "2021-12-03 14:29:52.186000",
            "updated_at": "2021-13-03 14:25:52.186000",
        },
    ]
    expected_list = [
        {
            "transaction_id": 1,
            "transaction_type": "PURCHASE",
            "sales_order_id": None,
            "purchase_order_id": 2,
        },
        {
            "transaction_id": 2,
            "transaction_type": "PURCHASE",
            "sales_order_id": None,
            "purchase_order_id": 5,
        },
    ]
    result = get_dim_transaction(input_list)
    assert result == expected_list
