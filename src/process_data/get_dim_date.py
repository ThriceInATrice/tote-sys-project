from datetime import date


def get_dim_date(data):
    """
    this is the last data processing function to run
    it takes the rest of the processed data and uses get_new_dates to find the date ids
    then it calls get_date_object to get a date object that can be easily queried
    then it uses the date object to construct a dim_date entry for each date
    and returns a list of those dictionaries
    """

    return [
        {
            "date_id": int(date_id),
            "year": get_date_object(date_id).year,
            "month": get_date_object(date_id).month,
            "day": get_date_object(date_id).day,
            "day_of_week": int(get_date_object(date_id).strftime("%w")),
            "day_name": get_date_object(date_id).strftime("%A"),
            "month_name": get_date_object(date_id).strftime("%B"),
            "quarter": (get_date_object(date_id).month // 3) + 1,
        }
        for date_id in get_new_dates(data)
    ]


def get_new_dates(data):
    """
    this function takes a list of dictionaries and searchs for the values under certain keys
    these keys are the only ones that have date ids as values
    it returns a list of the values to be used in get_dim_date
    """
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
    """
    this function turns a date id from the data processing into a datetime object
    so that it can be easily queried in get_dim_date to give the required information
    """

    return date(
        int(date_id_string[:4]), int(date_id_string[4:6]), int(date_id_string[6:])
    )
