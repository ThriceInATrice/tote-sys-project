from src.process_data.process_address import process_address


def test_process_address_accepts_list_of_dicts_as_input():
    input_list = [{'column1': '3'}, {'column2': '4'}]
    process_address(input_list)


def test_process_address_returns_correct_data():
    input_list = [
        {
            "address_id":	"1",
            "address_line_1":	"6826 Herzog Via",
            "address_line_2":	None,
            "district":	"Avon",
            "city":	"New Patienceburgh",
            "postal_code":	"28441",
            "country":	"Turkey",
            "phone":	"1803 637401",
            "created_at":	"2022-11-03 14:20:49.962000",
            "last_updated":	"2022-11-03 14:20:49.962000"
        }
    ]
    expected_query = f'''
        INSERT INTO dim_location
        (location_id, address_line_1, address_line_2,
        district, city, postal_code, country, phone)
        VALUES
        (1, "6826 Herzog Via", NULL, "Avon", "New Patienceburgh", "28441",
        "Turkey", "1803 637401");'''

    result = process_address(input_list)
    assert result == expected_query