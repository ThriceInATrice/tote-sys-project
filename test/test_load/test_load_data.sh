#!/usr/bin/env bash
 
cat > test_load_database.sql <<EOM
\c test_load_database

CREATE TABLE department (
    department_id INT PRIMARY KEY,
    department_name TEXT,
    location TEXT
);


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