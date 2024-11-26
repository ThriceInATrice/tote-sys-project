#!/usr/bin/env bash
 
cat > test_process_database.sql <<EOM
\c test_process_database

CREATE TABLE department (
    department_id INT PRIMARY KEY,
    department_name TEXT,
    location TEXT
);

INSERT INTO department 
(department_id, department_name, location)
VALUES 
(1, 'Sales', 'Manchester'), 
(2, 'Purchasing', 'Manchester')

RETURNING *;

CREATE TABLE address (
    address_id INT PRIMARY KEY,
    address_line_1 TEXT,
    address_line_2 TEXT,
    district TEXT,
    city TEXT,
    postal_code TEXT, 
    country TEXT,
    phone TEXT
);

INSERT INTO address 
(address_id, address_line_1, address_line_2, district, city, postal_code, country, phone)
VALUES 
(1, '6826 Herzog Via', 'None', 'Avon', 'New Patienceburgh', '28441', 'Turkey', '1803 637401'), 
(2, '179 Alexie Cliffs', 'None', 'None', 'Aliso Viejo', '99305-7380', 'San Marino', '9621 880720'),
(28, '079 Horacio Landing', 'None', 'None', 'Utica', '93045', 'Austria', '7772 084705'),
(15, '605 Haskell Trafficway', 'Axel Freeway', 'None', 'East Bobbie', '88253-4257', 'Heard Island and McDonald Islands', '9687 937447')
RETURNING *
;

EOM

cat > test/test_process_database.ini << EOM
[postgresql_test_process_database]
host=$PGHOST
port=5432
database=test_process_database
user=$USER
password=$PGPASSWORD
EOM

psql -c 'DROP DATABASE IF EXISTS test_process_database;'
psql -c 'CREATE DATABASE test_process_database;'
psql -f test_process_database.sql
rm test_process_database.sql

pytest --log-cli-level=INFO test/test_process_data -vvvrP
rm test_process_database.ini

if  [[ $PGUSER!='postgres_user' ]]; then psql -c "DROP DATABASE IF EXISTS test_process_database"; fi