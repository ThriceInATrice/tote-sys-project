from src.process_data.get_dim_design import get_dim_design


def test_process_design_returns_correct_data_for_single_dict():
    input_list = [
        {
            "design_id": 8,
            "created_at": "2022-11-03 14:20:49.962000",
            "last_updated": "2022-11-03 14:20:49.962000",
            "design_name": "Wooden",
            "file_location": "/usr",
            "file_name": "wooden-20220717-npgz.json",
        }
    ]
    expected_list = [
        {
            "design_id": 8,
            "design_name": "Wooden",
            "file_location": "/usr",
            "file_name": "wooden-20220717-npgz.json",
        }
    ]
    result = get_dim_design(input_list)
    assert result == expected_list


def test_process_design_returns_correct_types_for_single_dict_values():
    input_list = [
        {
            "design_id": 8,
            "created_at": "2022-11-03 14:20:49.962000",
            "last_updated": "2022-11-03 14:20:49.962000",
            "design_name": "Wooden",
            "file_location": "/usr",
            "file_name": "wooden-20220717-npgz.json",
        }
    ]

    result = get_dim_design(input_list)
    assert type(result[0]["design_id"]) == int
    assert type(result[0]["design_name"]) == str
    assert type(result[0]["file_location"]) == str
    assert type(result[0]["file_name"]) == str


def test_process_design_returns_correct_data_for_multiple_dicts():
    input_list = [
        {
            "design_id": 8,
            "created_at": "2022-11-03 14:20:49.962000",
            "last_updated": "2022-11-03 14:20:49.962000",
            "design_name": "Wooden",
            "file_location": "/usr",
            "file_name": "wooden-20220717-npgz.json",
        },
        {
            "design_id": 51,
            "created_at": "2023-01-12 18:50:09.935000",
            "last_updated": "2023-01-12 18:50:09.935000",
            "design_name": "Bronze",
            "file_location": "bronze-20221024-4dds.json",
            "file_name": "wooden-20220717-npgz.json",
        },
    ]
    expected_list = [
        {
            "design_id": 8,
            "design_name": "Wooden",
            "file_location": "/usr",
            "file_name": "wooden-20220717-npgz.json",
        },
        {
            "design_id": 51,
            "design_name": "Bronze",
            "file_location": "bronze-20221024-4dds.json",
            "file_name": "wooden-20220717-npgz.json",
        },
    ]
    result = get_dim_design(input_list)
    assert result == expected_list
