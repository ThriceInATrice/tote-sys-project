\c test_load_database

CREATE TABLE IF NOT EXISTS dim_counterparty (
    counterparty_id INT PRIMARY KEY,
    counterparty_legal_name TEXT,
    counterparty_legal_address_line_1 TEXT,
    counterparty_legal_address_line_2 TEXT,
    counterparty_legal_district TEXT,
    counterparty_legal_city TEXT,
    counterparty_legal_postal_code TEXT,
    counterparty_legal_country TEXT,
    counterparty_legal_phone_number TEXT
);

CREATE TABLE IF NOT EXISTS dim_currency (
    currency_id INT PRIMARY KEY,
    currency_code TEXT,
    currency_name TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    day_name TEXT,
    month_name TEXT,
    quarter INT
);

CREATE TABLE IF NOT EXISTS dim_design (
    design_id INT PRIMARY KEY,
    design_name TEXT,
    file_location TEXT,
    file_name TEXT
);

CREATE TABLE IF NOT EXISTS dim_location(
    location_id INT PRIMARY KEY,
    address_line_1 TEXT,
    address_line_2 TEXT,
    district TEXT,
    city TEXT,
    postal_code TEXT,
    country TEXT,
    phone TEXT
);

CREATE TABLE IF NOT EXISTS dim_payment_type(
    payment_type_id INT PRIMARY KEY,
    payment_type_name TEXT
);

CREATE TABLE IF NOT EXISTS dim_staff(
    staff_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    department_name TEXT,
    location TEXT,
    email_address TEXT
);

CREATE TABLE IF NOT EXISTS dim_transaction(
    transaction_id INT PRIMARY KEY,
    transaction_type TEXT,
    sales_order_id INT,
    purchase_order_id INT
);

CREATE TABLE IF NOT EXISTS fact_payment(
    payment_record_id SERIAL PRIMARY KEY,
    payment_id INT,
    created_date date,
    created_time time without time zone,
    last_updated_date date,
    last_updated_time time without time zone,
    transaction_id INT,
    counterparty_id INT,
    payment_amount numeric,
    currency_id INT,
    payment_type_id INT,
    paid boolean,
    payment_date date
);

CREATE TABLE IF NOT EXISTS fact_purchase_order(
    purchase_record_id SERIAL PRIMARY KEY,
    purchase_order_id INT,
    created_date date,
    created_time time without time zone,
    last_updated_date date,
    last_updated_time time without time zone,
    staff_id INT,
    counterparty_id INT,
    item_code TEXT,
    item_quantity INT,
    item_unit_price numeric,
    currency_id INT,
    agreed_delivery_date date,
    agreed_payment_date date,
    agreed_delivery_location_id INT
);

CREATE TABLE IF NOT EXISTS fact_sales_order(
    sales_record_id SERIAL PRIMARY KEY,
    sales_order_id INT,
    created_date date,
    created_time time without time zone,
    last_updated_date date,
    last_updated_time time without time zone,
    sales_staff_id INT,
    counterparty_id INT,
    units_sold INT,
    unit_price numeric(10, 2),
    currency_id INT,
    design_id INT,
    agreed_payment_date date,
    agreed_delivery_date date,
    agreed_delivery_location_id INT
);