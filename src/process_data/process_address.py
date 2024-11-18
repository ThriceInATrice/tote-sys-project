def process_address(address_data):
    update_command=f"""INSERT INTO dim_location (location_id, address_line_1, address_line_2, district, city, postal_code, country, phone) VALUES """
    for row in address_data:
        update_command += f"""{row["address_id"], 
                              row["address_line_1"], 
                              row["address_line_2"] if row["address_line_2"] else "NULL",
                              row["district"] if row["district"] else "NULL",
                              row["city"],
                              row["postal_code"],
                              row["country"],
                              row["phone"]})"""

    update_command = update_command[:-1]+";"
    return update_command