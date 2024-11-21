from src.load.insert_dim_counterparty import insert_dim_counterparty, get_dim_counterparty_insert_query


class TestGetDimCounterpartyInsertQuery:
    def test_function_returns_correct_insert_query_for_a_single_row(self):
        test_input = [
            {
                "counterparty_id": "1",
                "counterparty_legal_name": "Fahey and Sons",
                "legal_address_id": "15",
                "commercial_contact": "Micheal Toy",
                "delivery_contact": "Mrs. Lucy Runolfsdottir",
                "created_at": "2022-11-03 14:20:51.563000",
                "last_updated": "2022-11-03 14:20:51.563000",
            }
        ]
        expected_return = """
            INSERT INTO 
        """
        assert get_dim_counterparty_insert_query(test_input) == expected_return


    def test_function_returns_correct_insert_query_for_multiple_rows(self): 
        test_input = [
            {
                "counterparty_id": "1",
                "counterparty_legal_name": "Fahey and Sons",
                "legal_address_id": "15",
                "commercial_contact": "Micheal Toy",
                "delivery_contact": "Mrs. Lucy Runolfsdottir",
                "created_at": "2022-11-03 14:20:51.563000",
                "last_updated": "2022-11-03 14:20:51.563000",
            },
            {
                "counterparty_id": "2",
                "counterparty_legal_name": "Leannon, Predovic and Morar",
                "legal_address_id": "28",
                "commercial_contact": "Melba Sanford",
                "delivery_contact": "Jean Hane III",
                "created_at": "2022-11-03 14:20:51.563000",
                "last_updated": "2022-11-03 14:20:51.563000",
            },
        ]
        expected_return = """
            INSERT INTO dim_counterparty (
                counterparty_id, 
                counterparty_legal_name, 
                counterparty_legal_adress_line_1, 
                counterparty_legal_address_line_2,
                counterparty_legal_district,
                counterparty_legal_city,
                counterparty_legal_postal_code,
                counterparty_legal_country,
                counterparty_legal_phone_number
                )
            VALUES
            ()
        """
        assert get_dim_counterparty_insert_query(test_input) == expected_return

class TestInsertDimCounterparty:
    pass