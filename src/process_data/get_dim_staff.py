try:
    from src.process_data.connection import connect_to_db
except ImportError:
    from connection import connect_to_db
from botocore.exceptions import ParamValidationError
from botocore.errorfactory import ClientError


class DBCredentialsExportError(Exception):
    pass


class UnexpectedDimStaffError(Exception):
    pass

def escape_quotes(input_str):
    if "'" in input_str:
        return input_str.replace("'", "")
    else:
        return input_str

def get_dim_staff(credentials_id, staff_data):
    """
    This function takes a list of dictionaries representing rows in the staff tables in the origin database
    and returns them in the format required for the data warehouse
    to do so it queries the origin datebase for data about the departments
    then uses this data to construct dictionaries representing lines in the data warehouse
    """

    if not isinstance(staff_data, list):
        raise TypeError("Input must be a list")
    elif not all([type(item) == dict for item in staff_data]):
        raise TypeError("Input must be a list of dictionaries")

    try:
        query_string = (
            """SELECT department_id, department_name, location FROM department;"""
        )
        conn = connect_to_db(credentials_id)
        with conn.cursor() as cursor:
            cursor.execute(query_string)
            departments = cursor.fetchall()

        departments_columns = ("department_id", "department_name", "location")
        department_list = [
            dict(zip(departments_columns, department)) for department in departments
        ]

        return [
            {
                "staff_id": row["staff_id"],
                "first_name": row["first_name"],
                "last_name": escape_quotes(row["last_name"]),
                "department_name": [
                    department["department_name"]
                    for department in department_list
                    if department["department_id"] == int(row["department_id"])
                ][0],
                "location": [
                    department["location"]
                    for department in department_list
                    if department["department_id"] == int(row["department_id"])
                ][0],
                "email_address": row["email_address"],
            }
            for row in staff_data
        ]

    except ParamValidationError:
        raise DBCredentialsExportError(
            "Please enter export DB_CREDENTIALS_ID=[Your database credentials ID] into terminal."
        )
    except TypeError:
        raise DBCredentialsExportError(
            "Incorrect Credentials ID: Please export DB_CREDENTIALS_ID=[Your database credentials ID] into terminal."
        )
    except Exception as e:
        raise UnexpectedDimStaffError(f"Unexpected Error: {e}")
