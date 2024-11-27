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
    then return a list of new dictionaries in this form
    {
    "design_id": int,
    "design_name": str,
    "file_location": str,
    "file_name": str
    }
    """
    processed_design_data = []
    for row in design_data:
        dim_design =  {"design_id": int(row["design_id"]), 
                              "design_name": row["design_name"], 
                              "file_location": row["file_location"],
                              "file_name": row["file_name"]
                              }
        processed_design_data.append(dim_design)

    return processed_design_data  

