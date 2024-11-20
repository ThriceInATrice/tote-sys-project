#!/usr/bin/env bash
 
cat > test_database.sql <<EOM
\c test_database

CREATE TABLE test_table (
    test_id SERIAL PRIMARY KEY,
    
    test_text_1 TEXT,
    test_text_2 TEXT,
    test_bool BOOLEAN
);

INSERT INTO test_table 
(test_text_1, test_text_2, test_bool)
VALUES 
('A', 'a', true), 
('B', 'b', false), 
('C', 'c', true), 
('D', 'd', false),
('E', 'e', true),
('F', 'f', false)

RETURNING *;
EOM

cat > test/test_database.ini << EOM
[postgresql_test_database]
host=$PGHOST
port=5432
database=test_database
user=$USER
password=$PGPASSWORD
EOM

psql -c 'DROP DATABASE IF EXISTS test_database;'
psql -c 'CREATE DATABASE test_database;'
psql -f test_database.sql
rm test_database.sql

pytest test/test_extraction/test_get_new_data_from_database.py -vvvrP
pytest test/test_extraction/test_extract.py -vvvvrP

if  [[ $PGUSER!='postgres_user' ]]; then psql -c "DROP DATABASE IF EXISTS test_database"; fi