# raw data arrives like this:
# {
#     "extraction_time": str,
#     "data": {
#         "staff": [
#             {
#                 "staff_id": int,
#                 "first name": str,
#             }
#         ],
#         "currency": [
#             {
#                 "currency_id": int,
#                 "currency_code": str
#                 }
#         ]
#     }
# }
#
# we want to return this:
# {
#     "extraction_time": str,
#     "processed_data": {
#         "dim_staff": get_dim_staff(data["data"]["staff"]),
#         "dim_currency": get_dim_currency(data["data"]["currency"])
#     }
# }
#
# get_dim_staff is given a list of dictionaries like this
# with each dictionary representing a line in the staff table in the origin database
# [
#     {
#         "currency_id": int,
#         "currency_code": str,
#         "created_at": str,
#         "last_updated": str
#     }
# ]
#
# and should return a new list of dictionaries of a similar structure
# with each dictionary representing a line in the dim_staff table in the new database
# [
#     {
#         "currency_id": int,
#         "currency_code": str,
#         "currency name": str
#     }
# ]


def process_data(datetime_string, data_dict):
    pass
