def get_insert_query(table_name, row_list):
    if len(row_list):
        column_names = row_list[0].keys()

        value_string = ",\n".join(
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
INSERT INTO {table_name} ({", ".join(column_names)})
VALUES
{value_string}
;
"""
        return insert_query

    else:
        return None
