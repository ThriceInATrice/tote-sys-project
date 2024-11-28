def get_dim_location(address_data):
    """
    This function should take a list of dictionaries of this form:
        {
            "address_id": int,
            "address_line_1": str,
            "address_line_2": str,
            "district": str,
            "city": str,
            "postal_code": str,
            "country": str,
            "phone": str,
            "created_at": str,
            "last_updated": str
        }
    and return a list of new dictionaries in this form:
        {
            "location_id": int,
            "address_line_1": str,
            "address_line_2": str,
            "district": str,
            "city": str,
            "postal_code": str,
            "country": str,
            "phone": str
        }
    """

    return [
        {
            "location_id": int(row["address_id"]),
            "address_line_1": (
                None if row["address_line_1"] == "None" else row["address_line_1"]
            ),
            "address_line_2": (
                None if row["address_line_2"] == "None" else row["address_line_2"]
            ),
            "district": None if row["district"] == "None" else row["district"],
            "city": None if row["city"] == "None" else row["city"],
            "postal_code": None if row["postal_code"] == "None" else row["postal_code"],
            "country": None if row["country"] == "None" else row["country"],
            "phone": None if row["phone"] == "None" else row["phone"],
        }
        for row in address_data
    ]
