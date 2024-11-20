from src.process_data.get_fact_purchase_order import get_fact_purchase_order


def test_get_fact_purchase_order_returns_correct_item_for_single_dict():

    input_list = [{
        "purchase_order_id":	1,
        "created_at":	"2022-11-03 14:20:52.187000",
        "last_updated":	"2022-11-03 14:20:52.187000",
        "staff_id":	12,
        "counterparty_id":	11,
        "item_code":	"ZDOI5EA",
        "item_quantity":	371,
        "item_unit_price":	361.39,
        "currency_id":	2,
        "agreed_delivery_date":	"2022-11-09",
        "agreed_payment_date":	"2022-11-07",
        "agreed_delivery_location_id":	6
    },
    {
        "purchase_order_id": 15,
        "created_at": "2022-11-08 18:50:10.306000",
        "last_updated":	"2022-11-08 18:50:10.306000",
        "staff_id":	7,
        "counterparty_id": 15,
        "item_code": "53FS4LJ",
        "item_quantity": 217,
        "item_unit_price": 616.20,
        "currency_id": 2,
        "agreed_delivery_date": "2022-11-14",
        "agreed_payment_date": "2022-11-09",
        "agreed_delivery_location_id": 6
    }]
    expected_list = [{
        "purchase_order_id": 1,
        "created_date":	"2022-11-03",
        "created_time": "14:20:52.187000",
        "last_updated_date": "2022-11-03",
        "last_updated_time":  "14:20:52.187000",
        "staff_id":	12,
        "counterparty_id":	11,
        "item_code":	"ZDOI5EA",
        "item_quantity":	371,
        "item_unit_price":	361.39,
        "currency_id":	2,
        "agreed_delivery_date":	"2022-11-09",
        "agreed_payment_date":	"2022-11-07",
        "agreed_delivery_location_id":	6
    },
    {
        "purchase_order_id": 15,
        "created_date": "2022-11-08",
        "created_time": "18:50:10.306000",
        "last_updated_date": "2022-11-08",
        "last_updated_time": "18:50:10.306000",
        "staff_id":	7,
        "counterparty_id": 15,
        "item_code": "53FS4LJ",
        "item_quantity": 217,
        "item_unit_price": 616.20,
        "currency_id": 2,
        "agreed_delivery_date": "2022-11-14",
        "agreed_payment_date": "2022-11-09",
        "agreed_delivery_location_id": 6
    }]

    result = get_fact_purchase_order(input_list)
    assert result == expected_list

def test_get_fact_purchase_order_returns_correct_item_for_multiple_dicts():
    input_list = [{
        "purchase_order_id":	1,
        "created_at":	"2022-11-03 14:20:52.187000",
        "last_updated":	"2022-11-03 14:20:52.187000",
        "staff_id":	12,
        "counterparty_id":	11,
        "item_code":	"ZDOI5EA",
        "item_quantity":	371,
        "item_unit_price":	361.39,
        "currency_id":	2,
        "agreed_delivery_date":	"2022-11-09",
        "agreed_payment_date":	"2022-11-07",
        "agreed_delivery_location_id":	6
    }]
    expected_list = [{
        "purchase_order_id":	1,
        "created_date":	"2022-11-03",
        "created_time": "14:20:52.187000",
        "last_updated_date": "2022-11-03",
        "last_updated_time":  "14:20:52.187000",
        "staff_id":	12,
        "counterparty_id":	11,
        "item_code":	"ZDOI5EA",
        "item_quantity":	371,
        "item_unit_price":	361.39,
        "currency_id":	2,
        "agreed_delivery_date":	"2022-11-09",
        "agreed_payment_date":	"2022-11-07",
        "agreed_delivery_location_id":	6
    }]

    result = get_fact_purchase_order(input_list)
    assert result == expected_list