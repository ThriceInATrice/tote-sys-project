from datetime import date


def get_dim_date(data):
    new_dim_date_entries = []
    for date_id in get_new_dates(data):
        date_object = get_date_object(date_id)
        new_dim_date_entries.append(
            {
                "date_id": int(date_id),
                "year": date_object.year,
                "month": date_object.month,
                "day": date_object.day,
                "day_of_week": int(date_object.strftime("%w")),
                "day_name": date_object.strftime("%A"),
                "month_name": date_object.strftime("%B"),
                # "quarter":
            }
        )
    return new_dim_date_entries


def get_new_dates(data):
    new_dates = []
    date_keys = [
        "created_date",
        "last_updated_date",
        "agreed_delivery_date",
        "agreed_payment_date",
        "payment_date",
    ]

    for lines in data.values():
        for line in lines:
            for key, value in line.items():
                if key in date_keys and value not in new_dates:
                    new_dates.append(str(value))

    return new_dates


def get_date_object(date_id_string):
    return date(
        int(date_id_string[:4]), int(date_id_string[4:6]), int(date_id_string[6:])
    )
