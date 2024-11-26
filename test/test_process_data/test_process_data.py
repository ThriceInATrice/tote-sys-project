from src.process_data.process_data import lambda_handler, get_unprocessed_extractions
from src.process_data.processing_error import ProcessingError
import pytest, boto3, json, re
from moto import mock_aws
from unittest.mock import patch
from configparser import ConfigParser
from pprint import pprint

test_input = {
    "extraction_time": "2024-11-20 15:27:20.495299",
    "data": {
        "sales_order": [
            {
                "sales_order_id": "2",
                "created_at": "2022-11-03 14:20:52.186000",
                "last_updated": "2022-11-03 14:20:52.186000",
                "design_id": "3",
                "staff_id": "19",
                "counterparty_id": "8",
                "units_sold": "42972",
                "unit_price": "3.94",
                "currency_id": "2",
                "agreed_delivery_date": "2022-11-07",
                "agreed_payment_date": "2022-11-08",
                "agreed_delivery_location_id": "8",
            },
            {
                "sales_order_id": "3",
                "created_at": "2022-11-03 14:20:52.188000",
                "last_updated": "2022-11-03 14:20:52.188000",
                "design_id": "4",
                "staff_id": "10",
                "counterparty_id": "4",
                "units_sold": "65839",
                "unit_price": "2.91",
                "currency_id": "3",
                "agreed_delivery_date": "2022-11-06",
                "agreed_payment_date": "2022-11-07",
                "agreed_delivery_location_id": "19",
            },
        ],
        "transaction": [
            {
                "transaction_id": "1",
                "transaction_type": "PURCHASE",
                "sales_order_id": "None",
                "purchase_order_id": "2",
                "created_at": "2022-11-03 14:20:52.186000",
                "last_updated": "2022-11-03 14:20:52.186000",
            },
            {
                "transaction_id": "2",
                "transaction_type": "PURCHASE",
                "sales_order_id": "None",
                "purchase_order_id": "3",
                "created_at": "2022-11-03 14:20:52.187000",
                "last_updated": "2022-11-03 14:20:52.187000",
            },
        ],
        "department": [
            {
                "department_id": "1",
                "department_name": "Sales",
                "location": "Manchester",
                "manager": "Richard Roma",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
            {
                "department_id": "2",
                "department_name": "Purchasing",
                "location": "Manchester",
                "manager": "Naomi Lapaglia",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
        ],
        "staff": [
            {
                "staff_id": "1",
                "first_name": "Jeremie",
                "last_name": "Franey",
                "department_id": "2",
                "email_address": "jeremie.franey@terrifictotes.com",
                "created_at": "2022-11-03 14:20:51.563000",
                "last_updated": "2022-11-03 14:20:51.563000",
            },
            {
                "staff_id": "2",
                "first_name": "Deron",
                "last_name": "Beier",
                "department_id": "1",
                "email_address": "deron.beier@terrifictotes.com",
                "created_at": "2022-11-03 14:20:51.563000",
                "last_updated": "2022-11-03 14:20:51.563000",
            },
        ],
        "purchase_order": [
            {
                "purchase_order_id": "1",
                "created_at": "2022-11-03 14:20:52.187000",
                "last_updated": "2022-11-03 14:20:52.187000",
                "staff_id": "12",
                "counterparty_id": "11",
                "item_code": "ZDOI5EA",
                "item_quantity": "371",
                "item_unit_price": "361.39",
                "currency_id": "2",
                "agreed_delivery_date": "2022-11-09",
                "agreed_payment_date": "2022-11-07",
                "agreed_delivery_location_id": "6",
            },
            {
                "purchase_order_id": "2",
                "created_at": "2022-11-03 14:20:52.186000",
                "last_updated": "2022-11-03 14:20:52.186000",
                "staff_id": "20",
                "counterparty_id": "17",
                "item_code": "QLZLEXR",
                "item_quantity": "286",
                "item_unit_price": "199.04",
                "currency_id": "2",
                "agreed_delivery_date": "2022-11-04",
                "agreed_payment_date": "2022-11-07",
                "agreed_delivery_location_id": "8",
            },
        ],
        "counterparty": [
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
        ],
        "payment": [
            {
                "payment_id": "2",
                "created_at": "2022-11-03 14:20:52.187000",
                "last_updated": "2022-11-03 14:20:52.187000",
                "transaction_id": "2",
                "counterparty_id": "15",
                "payment_amount": "552548.62",
                "currency_id": "2",
                "payment_type_id": "3",
                "paid": "False",
                "payment_date": "2022-11-04",
                "company_ac_number": "67305075",
                "counterparty_ac_number": "31622269",
            },
            {
                "payment_id": "3",
                "created_at": "2022-11-03 14:20:52.186000",
                "last_updated": "2022-11-03 14:20:52.186000",
                "transaction_id": "3",
                "counterparty_id": "18",
                "payment_amount": "205952.22",
                "currency_id": "3",
                "payment_type_id": "1",
                "paid": "False",
                "payment_date": "2022-11-03",
                "company_ac_number": "81718079",
                "counterparty_ac_number": "47839086",
            },
        ],
        "currency": [
            {
                "currency_id": "1",
                "currency_code": "GBP",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
            {
                "currency_id": "2",
                "currency_code": "USD",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
        ],
        "payment_type": [
            {
                "payment_type_id": "1",
                "payment_type_name": "SALES_RECEIPT",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
            {
                "payment_type_id": "2",
                "payment_type_name": "SALES_REFUND",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
        ],
        "address": [
            {
                "address_id": "1",
                "address_line_1": "6826 Herzog Via",
                "address_line_2": "None",
                "district": "Avon",
                "city": "New Patienceburgh",
                "postal_code": "28441",
                "country": "Turkey",
                "phone": "1803 637401",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
            {
                "address_id": "2",
                "address_line_1": "179 Alexie Cliffs",
                "address_line_2": "None",
                "district": "None",
                "city": "Aliso Viejo",
                "postal_code": "99305-7380",
                "country": "San Marino",
                "phone": "9621 880720",
                "created_at": "2022-11-03 14:20:49.962000",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
        ],
        "design": [
            {
                "design_id": "8",
                "created_at": "2022-11-03 14:20:49.962000",
                "design_name": "Wooden",
                "file_location": "/usr",
                "file_name": "wooden-20220717-npgz.json",
                "last_updated": "2022-11-03 14:20:49.962000",
            },
            {
                "design_id": "51",
                "created_at": "2023-01-12 18:50:09.935000",
                "design_name": "Bronze",
                "file_location": "/private",
                "file_name": "bronze-20221024-4dds.json",
                "last_updated": "2023-01-12 18:50:09.935000",
            },
        ],
    },
}


expected_processed_data = {
    "dim_staff": [
        {
            "staff_id": "1",
            "first_name": "Jeremie",
            "last_name": "Franey",
            "department_name": "Purchasing",
            "location": "Manchester",
            "email_address": "jeremie.franey@terrifictotes.com",
        },
        {
            "staff_id": "2",
            "first_name": "Deron",
            "last_name": "Beier",
            "department_name": "Sales",
            "location": "Manchester",
            "email_address": "deron.beier@terrifictotes.com",
        },
    ],
    "dim_location": [
        {
            "location_id": "1",
            "address_line_1": "6826 Herzog Via",
            "address_line_2": "None",
            "district": "Avon",
            "city": "New Patienceburgh",
            "postal_code": "28441",
            "country": "Turkey",
            "phone": "1803 637401",
        },
        {
            "location_id": "2",
            "address_line_1": "179 Alexie Cliffs",
            "address_line_2": "None",
            "district": "None",
            "city": "Aliso Viejo",
            "postal_code": "99305-7380",
            "country": "San Marino",
            "phone": "9621 880720",
        },
    ],
    "dim_design": [
        {
            "design_id": "8",
            "design_name": "Wooden",
            "file_location": "/usr",
            "file_name": "wooden-20220717-npgz.json",
        },
        {
            "design_id": "51",
            "design_name": "Bronze",
            "file_location": "/private",
            "file_name": "bronze-20221024-4dds.json",
        },
    ],
    "dim_currency": [
        {"currency_id": "1", "currency_code": "GBP", "currency_name": "British Pound"},
        {"currency_id": "2", "currency_code": "USD", "currency_name": "US Dollar"},
    ],
    "dim_transaction": [
        {
            "transaction_id": "1",
            "transaction_type": "PURCHASE",
            "sales_order_id": "None",
            "purchase_order_id": "2",
        },
        {
            "transaction_id": "2",
            "transaction_type": "PURCHASE",
            "sales_order_id": "None",
            "purchase_order_id": "3",
        },
    ],
    "dim_payment_type": [
        {"payment_type_id": "1", "payment_type_name": "SALES_RECEIPT"},
        {"payment_type_id": "2", "payment_type_name": "SALES_REFUND"},
    ],
    "dim_counterparty": [
        {
            "counterparty_id": 1,
            "counterparty_legal_name": "Fahey and Sons",
            "counterparty_legal_address_line_1": "605 Haskell Trafficway",
            "counterparty_legal_address_line_2": "Axel Freeway",
            "counterparty_legal_district": "None",
            "counterparty_legal_city": "East Bobbie",
            "counterparty_legal_postal_code": "88253-4257",
            "counterparty_legal_country": "Heard Island and McDonald Islands",
            "counterparty_legal_phone_number": "9687 937447",
        },
        {
            "counterparty_id": 2,
            "counterparty_legal_name": "Leannon, Predovic and Morar",
            "counterparty_legal_address_line_1": "079 Horacio Landing",
            "counterparty_legal_address_line_2": "None",
            "counterparty_legal_district": "None",
            "counterparty_legal_city": "Utica",
            "counterparty_legal_postal_code": "93045",
            "counterparty_legal_country": "Austria",
            "counterparty_legal_phone_number": "7772 084705",
        },
    ],
    "fact_purchase_order": [
        {
            "purchase_order_id": "1",
            "created_date": "20221103",
            "created_time": "14:20:52.187000",
            "last_updated_date": "20221103",
            "last_updated_time": "14:20:52.187000",
            "staff_id": "12",
            "counterparty_id": "11",
            "item_code": "ZDOI5EA",
            "item_quantity": "371",
            "item_unit_price": "361.39",
            "currency_id": "2",
            "agreed_delivery_date": "20221109",
            "agreed_payment_date": "20221107",
            "agreed_delivery_location_id": "6",
        },
        {
            "purchase_order_id": "2",
            "created_date": "20221103",
            "created_time": "14:20:52.186000",
            "last_updated_date": "20221103",
            "last_updated_time": "14:20:52.186000",
            "staff_id": "20",
            "counterparty_id": "17",
            "item_code": "QLZLEXR",
            "item_quantity": "286",
            "item_unit_price": "199.04",
            "currency_id": "2",
            "agreed_delivery_date": "20221104",
            "agreed_payment_date": "20221107",
            "agreed_delivery_location_id": "8",
        },
    ],
    "fact_sales_order": [
        {
            "created_date": "20221103",
            "created_time": "14:20:52.186000",
            "last_updated_date": "20221103",
            "last_updated_time": "14:20:52.186000",
            "design_id": "3",
            "staff_id": "19",
            "counterparty_id": "8",
            "units_sold": "42972",
            "unit_price": "3.94",
            "currency_id": "2",
            "agreed_delivery_date": "20221107",
            "agreed_payment_date": "20221108",
            "agreed_delivery_location_id": "8",
        },
        {
            "created_date": "20221103",
            "created_time": "14:20:52.188000",
            "last_updated_date": "20221103",
            "last_updated_time": "14:20:52.188000",
            "design_id": "4",
            "staff_id": "10",
            "counterparty_id": "4",
            "units_sold": "65839",
            "unit_price": "2.91",
            "currency_id": "3",
            "agreed_delivery_date": "20221106",
            "agreed_payment_date": "20221107",
            "agreed_delivery_location_id": "19",
        },
    ],
    "fact_payment": [
        {
            "payment_id": "2",
            "created_time": "14:20:52.187000",
            "created_date": "20221103",
            "last_updated_time": "14:20:52.187000",
            "last_updated_date": "20221103",
            "transaction_id": "2",
            "counterparty_id": "15",
            "payment_amount": "552548.62",
            "currency_id": "2",
            "payment_type_id": "3",
            "paid": "False",
            "payment_date": "20221104",
        },
        {
            "payment_id": "3",
            "created_time": "14:20:52.186000",
            "created_date": "20221103",
            "last_updated_time": "14:20:52.186000",
            "last_updated_date": "20221103",
            "transaction_id": "3",
            "counterparty_id": "18",
            "payment_amount": "205952.22",
            "currency_id": "3",
            "payment_type_id": "1",
            "paid": "False",
            "payment_date": "20221103",
        },
    ],
    "dim_date": [
        {
            "date_id": 20221103,
            "year": 2022,
            "month": 11,
            "day": 3,
            "day_of_week": 4,
            "day_name": "Thursday",
            "month_name": "November",
            "quarter": 4,
        },
        {
            "date_id": 20221109,
            "year": 2022,
            "month": 11,
            "day": 9,
            "day_of_week": 3,
            "day_name": "Wednesday",
            "month_name": "November",
            "quarter": 4,
        },
        {
            "date_id": 20221107,
            "year": 2022,
            "month": 11,
            "day": 7,
            "day_of_week": 1,
            "day_name": "Monday",
            "month_name": "November",
            "quarter": 4,
        },
        {
            "date_id": 20221104,
            "year": 2022,
            "month": 11,
            "day": 4,
            "day_of_week": 5,
            "day_name": "Friday",
            "month_name": "November",
            "quarter": 4,
        },
        {
            "date_id": 20221108,
            "year": 2022,
            "month": 11,
            "day": 8,
            "day_of_week": 2,
            "day_name": "Tuesday",
            "month_name": "November",
            "quarter": 4,
        },
        {
            "date_id": 20221106,
            "year": 2022,
            "month": 11,
            "day": 6,
            "day_of_week": 0,
            "day_name": "Sunday",
            "month_name": "November",
            "quarter": 4,
        },
    ],
}

parser = ConfigParser()
parser.read("test/test_process_database.ini")
params = parser.items("postgresql_test_process_database")
config_dict = {param[0]: param[1] for param in params}


@mock_aws
class TestProcessData:
    def test_lambda_handler_processes_data_correctly(self):
        # create s3 client
        client = boto3.client("s3")

        # create mock secrets manager and store database credentials
        secrets_client = boto3.client("secretsmanager", region_name="eu-west-2")
        response = secrets_client.create_secret(
            Name="test_db_creds",
            Description="test mock secret",
            SecretString=str(json.dumps(config_dict)),
        )

        # create extraction_times bucket and populate with a single extraction time
        extraction_times_bucket_name = "extraction_times_bucket"
        extraction_times_key = "extraction_times.json"
        extraction_time = "2024-11-20 15:27:20.495299"
        extraction_times_body = json.dumps({"extraction_times": [extraction_time]})
        client.create_bucket(
            Bucket=extraction_times_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=extraction_times_bucket_name,
            Key=extraction_times_key,
            Body=extraction_times_body,
        )

        # create ingestion_bucket and populate with test_input data
        # key is generated from the extraction time in the same way as in extract.py
        ingestion_bucket_name = "ingestion_bucket"
        date_split = re.findall("[0-9]+", extraction_time)
        ingestion_key = "/".join(
            [date_split[0], date_split[1], date_split[2], extraction_time + ".json"]
        )

        ingestion_body = json.dumps(test_input)
        client.create_bucket(
            Bucket=ingestion_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=ingestion_bucket_name,
            Key=ingestion_key,
            Body=ingestion_body,
        )

        # create processed_extractions bucket and populate with an empty list
        processed_extractions_bucket_name = "processed_extractions_bucket"
        processed_extractions_key = "processed_extractions.json"
        processed_extractions_body = json.dumps({"extraction_times": []})
        client.create_bucket(
            Bucket=processed_extractions_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=processed_extractions_bucket_name,
            Key=processed_extractions_key,
            Body=processed_extractions_body,
        )

        # create processed_data bucket for the data to be placed into
        processed_data_bucket_name = "processed_data_bucket"
        client.create_bucket(
            Bucket=processed_data_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # lambda_handler is called with json of relevant information
        # context is a required field in aws lambda, but can be an empty json or dict
        event = {
            "credentials_id": "test_db_creds",
            "ingestion_bucket": "ingestion_bucket",
            "extraction_times_bucket": "extraction_times_bucket",
            "processed_data_bucket": "processed_data_bucket",
            "processed_extractions_bucket": "processed_extractions_bucket",
        }
        context = {}

        # call lambda handler
        lambda_handler(event, context)

        # check processed_data bucket to see if the data is present and processed correctly
        # this uses the same key as in the ingestion bucket so that we know which data it has come from
        response = client.get_object(
            Bucket=processed_data_bucket_name, Key=ingestion_key
        )
        processed_data_body = response["Body"]
        processed_data_bytes = processed_data_body.read()
        processed_data = json.loads(processed_data_bytes)
        assert processed_data["processed_data"] == expected_processed_data

        # check extraction time is in processed_extractions bucket correctly
        response = client.get_object(
            Bucket=processed_extractions_bucket_name, Key="processed_extractions.json"
        )
        processed_extractions_body = response["Body"]
        processed_extractions_bytes = processed_extractions_body.read()
        processed_extractions_dict = json.loads(processed_extractions_bytes)
        processed_extractions_list = processed_extractions_dict["extraction_times"]
        assert processed_extractions_list == [extraction_time]

    def test_process_data_can_handle_multiple_unprocessed_extractions(self):
        # make 2 dictionaries from the larger test dictionaries above
        test_data_1 = {
            "data": {key: [value[0]] for key, value in test_input["data"].items()}
        }
        test_data_2 = {
            "data": {key: [value[1]] for key, value in test_input["data"].items()}
        }
        expected_data_1 = {
            key: [value[0]] for key, value in expected_processed_data.items()
        }
        expected_data_1["dim_date"] = [
            {
                "date_id": 20221103,
                "day": 3,
                "day_name": "Thursday",
                "day_of_week": 4,
                "month": 11,
                "month_name": "November",
                "quarter": 4,
                "year": 2022,
            },
            {
                "date_id": 20221109,
                "day": 9,
                "day_name": "Wednesday",
                "day_of_week": 3,
                "month": 11,
                "month_name": "November",
                "quarter": 4,
                "year": 2022,
            },
            {
                "date_id": 20221107,
                "day": 7,
                "day_name": "Monday",
                "day_of_week": 1,
                "month": 11,
                "month_name": "November",
                "quarter": 4,
                "year": 2022,
            },
            {
                "date_id": 20221108,
                "day": 8,
                "day_name": "Tuesday",
                "day_of_week": 2,
                "month": 11,
                "month_name": "November",
                "quarter": 4,
                "year": 2022,
            },
            {
                "date_id": 20221104,
                "day": 4,
                "day_name": "Friday",
                "day_of_week": 5,
                "month": 11,
                "month_name": "November",
                "quarter": 4,
                "year": 2022,
            },
        ]

        expected_data_2 = {
            key: [value[1]] for key, value in expected_processed_data.items()
        }
        expected_data_2["dim_date"] = [
            {
                "date_id": 20221103,
                "day": 3,
                "day_name": "Thursday",
                "day_of_week": 4,
                "month": 11,
                "month_name": "November",
                "quarter": 4,
                "year": 2022,
            },
            {
                "date_id": 20221104,
                "day": 4,
                "day_name": "Friday",
                "day_of_week": 5,
                "month": 11,
                "month_name": "November",
                "quarter": 4,
                "year": 2022,
            },
            {
                "date_id": 20221107,
                "day": 7,
                "day_name": "Monday",
                "day_of_week": 1,
                "month": 11,
                "month_name": "November",
                "quarter": 4,
                "year": 2022,
            },
            {
                "date_id": 20221106,
                "day": 6,
                "day_name": "Sunday",
                "day_of_week": 0,
                "month": 11,
                "month_name": "November",
                "quarter": 4,
                "year": 2022,
            },
        ]

        # create s3 client
        client = boto3.client("s3")

        # create mock secrets manager and store database credentials
        secrets_client = boto3.client("secretsmanager", region_name="eu-west-2")
        response = secrets_client.create_secret(
            Name="test_db_creds",
            Description="test mock secret",
            SecretString=str(json.dumps(config_dict)),
        )

        # create extraction_times bucket and populate with 2 extraction times
        extraction_times_bucket_name = "extraction_times_bucket"
        extraction_times_key = "extraction_times.json"
        extraction_time_1 = "2024-11-20 15:00:00.000000"
        extraction_time_2 = "2024-11-21 15:00:00.000000"
        extraction_times_body = json.dumps(
            {"extraction_times": [extraction_time_1, extraction_time_2]}
        )
        client.create_bucket(
            Bucket=extraction_times_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=extraction_times_bucket_name,
            Key=extraction_times_key,
            Body=extraction_times_body,
        )

        # create ingestion_bucket and populate with test_data_1 and test_data_2
        # key is generated from the extraction time in the same way as in extract.py
        ingestion_bucket_name = "ingestion_bucket"
        client.create_bucket(
            Bucket=ingestion_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        date_split_1 = re.findall("[0-9]+", extraction_time_1)
        ingestion_key_1 = "/".join(
            [
                date_split_1[0],
                date_split_1[1],
                date_split_1[2],
                extraction_time_1 + ".json",
            ]
        )
        ingestion_body_1 = json.dumps(test_data_1)
        client.put_object(
            Bucket=ingestion_bucket_name,
            Key=ingestion_key_1,
            Body=ingestion_body_1,
        )

        date_split_2 = re.findall("[0-9]+", extraction_time_2)
        ingestion_key_2 = "/".join(
            [
                date_split_2[0],
                date_split_2[1],
                date_split_2[2],
                extraction_time_2 + ".json",
            ]
        )
        ingestion_body_2 = json.dumps(test_data_2)
        client.put_object(
            Bucket=ingestion_bucket_name,
            Key=ingestion_key_2,
            Body=ingestion_body_2,
        )

        # create processed_extractions bucket and populate with an empty list
        processed_extractions_bucket_name = "processed_extractions_bucket"
        processed_extractions_key = "processed_extractions.json"
        processed_extractions_body = json.dumps({"extraction_times": []})
        client.create_bucket(
            Bucket=processed_extractions_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=processed_extractions_bucket_name,
            Key=processed_extractions_key,
            Body=processed_extractions_body,
        )

        # create processed_data bucket for the data to be placed into
        processed_data_bucket_name = "processed_data_bucket"
        client.create_bucket(
            Bucket=processed_data_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # lambda_handler is called with json of relevant information
        # context is a required field in aws lambda, but can be an empty json or dict
        event = {
            "credentials_id": "test_db_creds",
            "ingestion_bucket": "ingestion_bucket",
            "extraction_times_bucket": "extraction_times_bucket",
            "processed_data_bucket": "processed_data_bucket",
            "processed_extractions_bucket": "processed_extractions_bucket",
        }
        context = {}

        # call lambda handler
        lambda_handler(event, context)

        # check processed_data bucket to see if the data is present and processed correctly
        # this uses the same key as in the ingestion bucket so that we know which data it has come from
        response = client.get_object(
            Bucket=processed_data_bucket_name, Key=ingestion_key_1
        )
        processed_data_body = response["Body"]
        processed_data_bytes = processed_data_body.read()
        processed_data = json.loads(processed_data_bytes)
        pprint(f"PROCESSED DATA: {processed_data}")
        pprint(f"EXPECTED DATA: {expected_data_1}")
        assert processed_data["processed_data"] == expected_data_1

        response = client.get_object(
            Bucket=processed_data_bucket_name, Key=ingestion_key_2
        )
        processed_data_body = response["Body"]
        processed_data_bytes = processed_data_body.read()
        processed_data = json.loads(processed_data_bytes)
        pprint(f"PROCESSED DATA: {processed_data}")
        pprint(f"EXPECTED DATA: {expected_data_2}")
        assert processed_data["processed_data"] == expected_data_2

        # check extraction time is in processed_extractions bucket correctly
        response = client.get_object(
            Bucket=processed_extractions_bucket_name, Key="processed_extractions.json"
        )
        processed_extractions_body = response["Body"]
        processed_extractions_bytes = processed_extractions_body.read()
        processed_extractions_dict = json.loads(processed_extractions_bytes)
        processed_extractions_list = processed_extractions_dict["extraction_times"]
        assert processed_extractions_list == [extraction_time_1, extraction_time_2]


@mock_aws
class TestGetUnprocessedData:

    def test_function_returns_times_that_have_not_been_processed(self):
        # create s3 client
        client = boto3.client("s3")

        # create and populate extraction_times bucket
        extraction_times_bucket_name = "extraction_times_bucket"
        extraction_times_key = "extraction_times.json"
        extraction_times_body = json.dumps({"extraction_times": ["today"]})
        client.create_bucket(
            Bucket=extraction_times_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=extraction_times_bucket_name,
            Key=extraction_times_key,
            Body=extraction_times_body,
        )

        # create and populate processed_extractions bucket
        processed_extractions_bucket_name = "processed_extractions_bucket"
        processed_extractions_key = "processed_extractions.json"
        processed_extractions_body = json.dumps({"extraction_times": []})
        client.create_bucket(
            Bucket=processed_extractions_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=processed_extractions_bucket_name,
            Key=processed_extractions_key,
            Body=processed_extractions_body,
        )

        # event contains relevant information for the function to opperate
        event = {
            "credentials_id": "test_db_creds",
            "ingestion_bucket": "ingestion_bucket",
            "extraction_times_bucket": "extraction_times_bucket",
            "processed_data_bucket": "processed_data_bucket",
            "processed_extractions_bucket": "processed_extractions_bucket",
        }

        # assert that the function returns the string from extraction_times
        assert get_unprocessed_extractions(event) == ["today"]

    def test_function_returns_empty_list_when_all_times_have_been_processed(self):
        # create s3 client
        client = boto3.client("s3")

        # create and populate extraction_times bucket
        extraction_times_bucket_name = "extraction_times_bucket"
        extraction_times_key = "extraction_times.json"
        extraction_times_body = json.dumps({"extraction_times": ["today"]})
        client.create_bucket(
            Bucket=extraction_times_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=extraction_times_bucket_name,
            Key=extraction_times_key,
            Body=extraction_times_body,
        )

        # create and populate processed_extractions bucket
        processed_extractions_bucket_name = "processed_extractions_bucket"
        processed_extractions_key = "processed_extractions.json"
        processed_extractions_body = json.dumps({"extraction_times": ["today"]})
        client.create_bucket(
            Bucket=processed_extractions_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=processed_extractions_bucket_name,
            Key=processed_extractions_key,
            Body=processed_extractions_body,
        )

        # event contains relevant information for the function to opperate
        event = {
            "credentials_id": "test_db_creds",
            "ingestion_bucket": "ingestion_bucket",
            "extraction_times_bucket": "extraction_times_bucket",
            "processed_data_bucket": "processed_data_bucket",
            "processed_extractions_bucket": "processed_extractions_bucket",
        }

        # assert that the function returns an empty list
        assert get_unprocessed_extractions(event) == []

    def test_function_raises_error_if_there_are_unexpected_entries_in_processed_extractions(
        self,
    ):
        # create s3 client
        client = boto3.client("s3")

        # create and populate extraction_times bucket
        extraction_times_bucket_name = "extraction_times_bucket"
        extraction_times_key = "extraction_times.json"
        extraction_times_body = json.dumps({"extraction_times": []})
        client.create_bucket(
            Bucket=extraction_times_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=extraction_times_bucket_name,
            Key=extraction_times_key,
            Body=extraction_times_body,
        )

        # create and populate processed_extractions bucket
        processed_extractions_bucket_name = "processed_extractions_bucket"
        processed_extractions_key = "processed_extractions.json"
        processed_extractions_body = json.dumps({"extraction_times": ["today"]})
        client.create_bucket(
            Bucket=processed_extractions_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=processed_extractions_bucket_name,
            Key=processed_extractions_key,
            Body=processed_extractions_body,
        )

        # event contains relevant information for the function to opperate
        event = {
            "credentials_id": "test_db_creds",
            "ingestion_bucket": "ingestion_bucket",
            "extraction_times_bucket": "extraction_times_bucket",
            "processed_data_bucket": "processed_data_bucket",
            "processed_extractions_bucket": "processed_extractions_bucket",
        }

        # assert functions raises error correctly
        with pytest.raises(ProcessingError):
            get_unprocessed_extractions(event) == []

    def test_function_creates_empty_json_if_processed_extractions_bucket_is_empty(self):
        # create s3 client
        client = boto3.client("s3")

        # create and populate extraction_times bucket
        extraction_times_bucket_name = "extraction_times_bucket"
        extraction_times_key = "extraction_times.json"
        extraction_times_body = json.dumps({"extraction_times": ["today"]})
        client.create_bucket(
            Bucket=extraction_times_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        client.put_object(
            Bucket=extraction_times_bucket_name,
            Key=extraction_times_key,
            Body=extraction_times_body,
        )

        # create processed_extractions bucket with no json inside
        processed_extractions_bucket_name = "processed_extractions_bucket"
        client.create_bucket(
            Bucket=processed_extractions_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # event contains relevant information for the function to opperate
        event = {
            "credentials_id": "test_db_creds",
            "ingestion_bucket": "ingestion_bucket",
            "extraction_times_bucket": "extraction_times_bucket",
            "processed_data_bucket": "processed_data_bucket",
            "processed_extractions_bucket": "processed_extractions_bucket",
        }

        # assert that the function returns the string from extraction_times
        assert get_unprocessed_extractions(event) == ["today"]
