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

    return [
        {
            "transaction_id": row["transaction_id"],
            "transaction_type": row["transaction_type"],
            "sales_order_id": row["sales_order_id"],
            "purchase_order_id": row["purchase_order_id"],
        }
        for row in transaction_data
    ]
