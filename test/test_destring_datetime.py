from src.extraction.get_new_data_from_database import destring_timestamp
from datetime import datetime
import pytest

def test_func_destrings_correctly():
    now = datetime.now()
    now_string= str(now)
    assert destring_timestamp(now_string) == now

def test_func_fails_correctly_when_given_wrong_input():
    with pytest.raises(Exception):
        destring_timestamp("hello_world")