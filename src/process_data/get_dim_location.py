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
            "location_id": row["address_id"],
            "address_line_1": row["address_line_1"],
            "address_line_2": row["address_line_2"],
            "district": row["district"],
            "city": row["city"],
            "postal_code": row["postal_code"],
            "country": row["country"],
            "phone": row["phone"],
        }
        for row in address_data
    ]
