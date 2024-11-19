from src.process_data.connection import connect_to_db


def get_dim_staff(credentials_id, staff_data):
    """
    This function should take a list of dictionaries of this form:
    {
    "staff_id": int,
    "first_name": str,
    "last_name": str,
    "department_id": int,
    "email_address": str,
    "created_at": str,
    "last_updated": str
    }
    then return a list of new dictionaries in this form
    {
    "staff_id": int,
    "first_name": str,
    "last_name": str,
    "department_name": str,
    "location": str,
    "email_address": str,
    }
    """

    query_string = (
        """SELECT department_id, department_name, location FROM department LIMIT 10;"""
    )

    conn = connect_to_db(credentials_id)
    with conn.cursor() as cursor:
        cursor.execute(query_string)
        departments = cursor.fetchall()

    departments_columns = ("department_id", "department_name", "location")
    department_list = []
    for department in departments:
        department_list.append(dict(zip(departments_columns, department)))

    # print('this is zipped departments >', department_list)

    processed_staff_data = []
    for row in staff_data:
        id = "department_id"
        name = "department_name"
        location = "location"
        department_name = [d[name] for d in department_list if d[id] == row[id]][0]
        department_location = [
            d[location] for d in department_list if d[id] == row[id]
        ][0]
        dim_staff = {
            "staff_id": row["staff_id"],
            "first_name": row["first_name"],
            "last_name": row["last_name"],
            "department_name": department_name,
            "location": department_location,
            "email_address": row["email_address"],
        }
        processed_staff_data.append(dim_staff)

    return processed_staff_data
