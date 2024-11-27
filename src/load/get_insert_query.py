def get_insert_query(table_name, row_list):
    """
    this function takes the data for one of the tables in the warehouse
    and returns the sql code to insert that data, designed so they can be
    concatonated into a single string and run at the same time
    """

    if len(row_list):
        columns = row_list[0].keys()
        column_names = ", ".join(columns)

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
                        for column in columns
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
        print(f"INSERT QUEREY: {insert_query}")
        return insert_query

    else:
        return None
