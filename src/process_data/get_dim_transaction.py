def get_dim_transaction(transaction_data):
    """
    This function should take a list of dictionaries of this form:
        {
            "transaction_id": int,
            "transaction_type": str,
            "sales_order_id": int,
            "purchase_order_id": int,
            "created_at": str,
            "updated_at": str
        }
    then return a list of new dictionaries in this form
        {
            "transaction_id": int,
            "transaction_type": str,
            "sales_order_id": int,
            "purchase_order_id": int,
        }
    """

    processed_transaction = []
    for row in transaction_data:
        dim_transaction =  {
            "transaction_id": int(row["transaction_id"]), 
            "transaction_type": row["transaction_type"], 
            "sales_order_id": None if row["sales_order_id"] in ["None", None] else int(row["sales_order_id"]),
            "purchase_order_id": None if row["purchase_order_id"] == ["None", None] else int(row["purchase_order_id"])
        }

        processed_transaction.append(dim_transaction)

    return processed_transaction