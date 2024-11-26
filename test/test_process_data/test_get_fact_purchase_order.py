from src.process_data.get_fact_purchase_order import get_fact_purchase_order
import pytest


def test_get_fact_purchase_order_returns_correct_item_for_single_dict():

    input_list = [
        {
            "purchase_order_id": 1,
            "created_at": "2022-11-03 14:20:52.187000",
            "last_updated": "2022-11-03 14:20:52.187000",
            "staff_id": 12,
            "counterparty_id": 11,
            "item_code": "ZDOI5EA",
            "item_quantity": 371,
            "item_unit_price": 361.39,
            "currency_id": 2,
            "agreed_delivery_date": "2022-11-09",
            "agreed_payment_date": "2022-11-07",
            "agreed_delivery_location_id": 6,
        }
    ]
    expected_list = [
        {
            "purchase_order_id": 1,
            "created_date": "20221103",
            "created_time": "14:20:52.187000",
            "last_updated_date": "20221103",
            "last_updated_time": "14:20:52.187000",
            "staff_id": 12,
            "counterparty_id": 11,
            "item_code": "ZDOI5EA",
            "item_quantity": 371,
            "item_unit_price": 361.39,
            "currency_id": 2,
            "agreed_delivery_date": "20221109",
            "agreed_payment_date": "20221107",
            "agreed_delivery_location_id": 6,
        }
    ]

    result = get_fact_purchase_order(input_list)
    assert result == expected_list


def test_get_fact_purchase_order_returns_correct_item_for_multiple_dicts():

    input_list = [
        {
            "purchase_order_id": 1,
            "created_at": "2022-11-03 14:20:52.187000",
            "last_updated": "2022-11-03 14:20:52.187000",
            "staff_id": 12,
            "counterparty_id": 11,
            "item_code": "ZDOI5EA",
            "item_quantity": 371,
            "item_unit_price": 361.39,
            "currency_id": 2,
            "agreed_delivery_date": "2022-11-09",
            "agreed_payment_date": "2022-11-07",
            "agreed_delivery_location_id": 6,
        },
        {
            "purchase_order_id": 15,
            "created_at": "2022-11-08 18:50:10.306000",
            "last_updated": "2022-11-08 18:50:10.306000",
            "staff_id": 7,
            "counterparty_id": 15,
            "item_code": "53FS4LJ",
            "item_quantity": 217,
            "item_unit_price": 616.20,
            "currency_id": 2,
            "agreed_delivery_date": "2022-11-14",
            "agreed_payment_date": "2022-11-09",
            "agreed_delivery_location_id": 6,
        },
    ]
    expected_list = [
        {
            "purchase_order_id": 1,
            "created_date": "20221103",
            "created_time": "14:20:52.187000",
            "last_updated_date": "20221103",
            "last_updated_time": "14:20:52.187000",
            "staff_id": 12,
            "counterparty_id": 11,
            "item_code": "ZDOI5EA",
            "item_quantity": 371,
            "item_unit_price": 361.39,
            "currency_id": 2,
            "agreed_delivery_date": "20221109",
            "agreed_payment_date": "20221107",
            "agreed_delivery_location_id": 6,
        },
        {
            "purchase_order_id": 15,
            "created_date": "20221108",
            "created_time": "18:50:10.306000",
            "last_updated_date": "20221108",
            "last_updated_time": "18:50:10.306000",
            "staff_id": 7,
            "counterparty_id": 15,
            "item_code": "53FS4LJ",
            "item_quantity": 217,
            "item_unit_price": 616.20,
            "currency_id": 2,
            "agreed_delivery_date": "20221114",
            "agreed_payment_date": "20221109",
            "agreed_delivery_location_id": 6,
        },
    ]

    input_list = [
        {
            "purchase_order_id": 1,
            "created_at": "2022-11-03 14:20:52.187000",
            "last_updated": "2022-11-03 14:20:52.187000",
            "staff_id": 12,
            "counterparty_id": 11,
            "item_code": "ZDOI5EA",
            "item_quantity": 371,
            "item_unit_price": 361.39,
            "currency_id": 2,
            "agreed_delivery_date": "2022-11-09",
            "agreed_payment_date": "2022-11-07",
            "agreed_delivery_location_id": 6,
        }
    ]
    expected_list = [
        {
            "purchase_order_id": 1,
            "created_date": "20221103",
            "created_time": "14:20:52.187000",
            "last_updated_date": "20221103",
            "last_updated_time": "14:20:52.187000",
            "staff_id": 12,
            "counterparty_id": 11,
            "item_code": "ZDOI5EA",
            "item_quantity": 371,
            "item_unit_price": 361.39,
            "currency_id": 2,
            "agreed_delivery_date": "20221109",
            "agreed_payment_date": "20221107",
            "agreed_delivery_location_id": 6,
        }
    ]

    result = get_fact_purchase_order(input_list)
    assert result == expected_list


def test_get_fact_purchase_order_returns_single_dict_with_values_of_correct_type():
    input_list = [
        {
            "purchase_order_id": 1,
            "created_at": "2022-11-03 14:20:52.187000",
            "last_updated": "2022-11-03 14:20:52.187000",
            "staff_id": 12,
            "counterparty_id": 11,
            "item_code": "ZDOI5EA",
            "item_quantity": 371,
            "item_unit_price": 361.39,
            "currency_id": 2,
            "agreed_delivery_date": "2022-11-09",
            "agreed_payment_date": "2022-11-07",
            "agreed_delivery_location_id": 6,
        }
    ]

    output_list = get_fact_purchase_order(input_list)

    assert type(output_list[0]["purchase_order_id"]) == int
    assert type(output_list[0]["created_date"]) == str
    assert type(output_list[0]["created_time"]) == str
    assert type(output_list[0]["last_updated_date"]) == str
    assert type(output_list[0]["last_updated_time"]) == str
    assert type(output_list[0]["staff_id"]) == int
    assert type(output_list[0]["counterparty_id"]) == int
    assert type(output_list[0]["item_code"]) == str
    assert type(output_list[0]["item_quantity"]) == int
    assert type(output_list[0]["item_unit_price"]) == float
    assert type(output_list[0]["currency_id"]) == int
    assert type(output_list[0]["agreed_delivery_date"]) == str
    assert type(output_list[0]["agreed_payment_date"]) == str
    assert type(output_list[0]["agreed_delivery_location_id"]) == int


def test_get_fact_purchase_order_returns_multiple_dicts_with_values_of_correct_type():
    input_list = [
        {
            "purchase_order_id": 1,
            "created_at": "2022-11-03 14:20:52.187000",
            "last_updated": "2022-11-03 14:20:52.187000",
            "staff_id": 12,
            "counterparty_id": 11,
            "item_code": "ZDOI5EA",
            "item_quantity": 371,
            "item_unit_price": 361.39,
            "currency_id": 2,
            "agreed_delivery_date": "2022-11-09",
            "agreed_payment_date": "2022-11-07",
            "agreed_delivery_location_id": 6,
        },
        {
            "purchase_order_id": 15,
            "created_at": "2022-11-08 18:50:10.306000",
            "last_updated": "2022-11-08 18:50:10.306000",
            "staff_id": 7,
            "counterparty_id": 15,
            "item_code": "53FS4LJ",
            "item_quantity": 217,
            "item_unit_price": 616.20,
            "currency_id": 2,
            "agreed_delivery_date": "2022-11-14",
            "agreed_payment_date": "2022-11-09",
            "agreed_delivery_location_id": 6,
        },
    ]

    output_list = get_fact_purchase_order(input_list)

    for row in output_list:
        assert type(row["purchase_order_id"]) == int
        assert type(row["created_date"]) == str
        assert type(row["created_time"]) == str
        assert type(row["last_updated_date"]) == str
        assert type(row["last_updated_time"]) == str
        assert type(row["staff_id"]) == int
        assert type(row["counterparty_id"]) == int
        assert type(row["item_code"]) == str
        assert type(row["item_quantity"]) == int
        assert type(row["item_unit_price"]) == float
        assert type(row["currency_id"]) == int
        assert type(row["agreed_delivery_date"]) == str
        assert type(row["agreed_payment_date"]) == str
        assert type(row["agreed_delivery_location_id"]) == int


def test_get_purchase_order_raises_error_if_not_list_type_input():
    input_list = 3
    with pytest.raises(TypeError, match="Input must be a list"):
        get_fact_purchase_order(input_list)


def test_get_purchase_order_raises_error_if_list_contains_non_dictionary_types():
    input_list = [1, {"test": 1}, 2]
    with pytest.raises(TypeError, match="Input must be a list of dictionaries"):
        get_fact_purchase_order(input_list)


def test_get_fact_purchase_order_returns_correct_item_for_multiple_dicts():
    input_list = [
        {
            "purchase_order_id": 1,
            "created_at": "2022-11-03 14:20:52.187000",
            "last_updated": "2022-11-03 14:20:52.187000",
            "staff_id": 12,
            "counterparty_id": 11,
            "item_code": "ZDOI5EA",
            "item_quantity": 371,
            "item_unit_price": 361.39,
            "currency_id": 2,
            "agreed_delivery_date": "2022-11-09",
            "agreed_payment_date": "2022-11-07",
            "agreed_delivery_location_id": 6,
        },
        {
            "purchase_order_id": 15,
            "created_at": "2022-11-08 18:50:10.306000",
            "last_updated": "2022-11-08 18:50:10.306000",
            "staff_id": 7,
            "counterparty_id": 15,
            "item_code": "53FS4LJ",
            "item_quantity": 217,
            "item_unit_price": 616.20,
            "currency_id": 2,
            "agreed_delivery_date": "2022-11-14",
            "agreed_payment_date": "2022-11-09",
            "agreed_delivery_location_id": 6,
        },
    ]
    expected_list = [
        {
            "purchase_order_id": 1,
            "created_date": "20221103",
            "created_time": "14:20:52.187000",
            "last_updated_date": "20221103",
            "last_updated_time": "14:20:52.187000",
            "staff_id": 12,
            "counterparty_id": 11,
            "item_code": "ZDOI5EA",
            "item_quantity": 371,
            "item_unit_price": 361.39,
            "currency_id": 2,
            "agreed_delivery_date": "20221109",
            "agreed_payment_date": "20221107",
            "agreed_delivery_location_id": 6,
        },
        {
            "purchase_order_id": 15,
            "created_date": "20221108",
            "created_time": "18:50:10.306000",
            "last_updated_date": "20221108",
            "last_updated_time": "18:50:10.306000",
            "staff_id": 7,
            "counterparty_id": 15,
            "item_code": "53FS4LJ",
            "item_quantity": 217,
            "item_unit_price": 616.20,
            "currency_id": 2,
            "agreed_delivery_date": "20221114",
            "agreed_payment_date": "20221109",
            "agreed_delivery_location_id": 6,
        },
    ]

    result = get_fact_purchase_order(input_list)
    assert result == expected_list
