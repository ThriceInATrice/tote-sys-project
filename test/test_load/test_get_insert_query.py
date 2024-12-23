from src.load.get_insert_query import get_insert_query



class TestGetInsertQuery:
    def test_func_returns_none_when_there_are_no_rows(self):
        assert (get_insert_query("dim_staff", [])) == None

    def test_func_returns_corrrect_insert_for_arbitrary_input(self):
        test_table_name = "test_table"
        test_row_list = [{"column_1": 1, "column_2": "hello world"}]
        expected_return = """
INSERT INTO test_table (column_1, column_2)
VALUES
(1, 'hello world')
ON CONFLICT DO NOTHING
;

"""
        assert get_insert_query(test_table_name, test_row_list) == expected_return

    def test_func_can_handle_multiple_rows(self):
        test_table_name = "test_table"
        test_row_list = [
            {"column_1": 1, "column_2": "hello world"},
            {"column_1": 2, "column_2": "hello worm"},
            {"column_1": 3, "column_2": "print(hello world)"},
        ]
        expected_return = """
INSERT INTO test_table (column_1, column_2)
VALUES
(1, 'hello world'),
(2, 'hello worm'),
(3, 'print(hello world)')
ON CONFLICT DO NOTHING
;

"""
        assert get_insert_query(test_table_name, test_row_list) == expected_return

    def test_function_handles_apostrophes_correctly(self):
        test_table_name = "test_table"
        test_row_list = [
            {"column_1": "John's cat", "column_2": "O'Keefe"},
            {"column_1": "it's too far, isn't it?", "column_2": "'ello world"},

        ]
        expected_return = """
INSERT INTO test_table (column_1, column_2)
VALUES
('John''s cat', 'O''Keefe'),
('it''s too far, isn''t it?', '''ello world')
ON CONFLICT DO NOTHING
;

"""

        assert get_insert_query(test_table_name, test_row_list) == expected_return

    def test_func_processes_multiple_tables_correctly(self):

        test_data = {
            "processed_data":{
                "table_1":[
                    {
                        "row_1": 1,
                        "row_2": 2
                    }, {
                        "row_1": 3,
                        "row_2": 4
                    }
                ],
                "table_2": [
                    {
                        "row_3": 5,
                        "row_4": 6
                    },
                    {
                        "row_3": 7,
                        "row_4": 8
                    }
                ]
            }
        }

        expected_return = """
INSERT INTO table_1 (row_1, row_2)
VALUES
(1, 2),
(3, 4)
ON CONFLICT DO NOTHING
;



INSERT INTO table_2 (row_3, row_4)
VALUES
(5, 6),
(7, 8)
ON CONFLICT DO NOTHING
;

"""

        query_str = f"\n".join(
                [
                    get_insert_query(table_name, row_list)
                    for table_name, row_list in test_data["processed_data"].items()
                ]
            )
        
        assert query_str == expected_return