def process_address(address_data):
    update_command=f"""
        INSERT INTO dim_currency (location_id, address_line_1, address_line_2,
        district, city, postal_code, country, phone)
        VALUES

    """
    for row in address_data:
        update_command += f"""({row["location"], 
                              row["address_line_1"], 
                              row["address_line_2"]})"""

    update_command = update_command[:-1]+";"
    return update_command