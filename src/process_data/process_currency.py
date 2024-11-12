from src.connection import connect_to_db

def process_currency(currency_data):
    update_command=f"""
        INSERT INTO dim_currency (currency_id, currency_code, currency_name)
        VALUES
        
    """
    for row in currency_data:
        update_command += f"({row["currency_id"], row["currency_code"], get_currency_name(row["currency_code"])}),"

    update_command = update_command[:-1]+";"
    return update_command
            

#func to use a currency code to get a currency name from a currency api
def get_currency_name(currnecy_code):
    pass
            
