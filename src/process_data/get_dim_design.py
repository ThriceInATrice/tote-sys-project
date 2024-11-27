def get_dim_design(design_data):
    """
    This function should take a list of dictionaries of this form:
        {
            "design_id": int,
            "created_at": str,
            "last_updated": str,
            "design_name": str,
            "file_location": str,
            "file_name": str
        }

    and return a list of new dictionaries in this form:
        {
            "design_id": int,
            "design_name": str,
            "file_location": str,
            "file_name": str
        }
    """

    return [
        {
            "design_id": row["design_id"],
            "design_name": row["design_name"],
            "file_location": row["file_location"],
            "file_name": row["file_name"],
        }
        for row in design_data
    ]
