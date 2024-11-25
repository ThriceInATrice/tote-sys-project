def get_insert_query(table_name, row_list):
    """ 
    this function takes the data for one of the tables in the warehouse
    and returns the sql code to insert that data, designed so they can be 
    concatonated into a single string and run at the same time
    """
    
    if len(row_list):
        column_names = ", ".join(row_list[0].keys())

        values = ",\n".join(
            [
                "("
                + ", ".join(
                    [
                        (
                            f"'{row[column]}'"
                            if type(row[column]) == str
                            else str(row[column])
                        )
                        for column in column_names
                    ]
                )
                + ")"
                for row in row_list
            ]
        )

        insert_query = f"""
INSERT INTO {table_name} ({column_names})
VALUES
{values}
;

"""
        return insert_query

    else:
        return None