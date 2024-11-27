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
;

"""
        assert get_insert_query(test_table_name, test_row_list) == expected_return
