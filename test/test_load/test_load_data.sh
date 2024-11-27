#!/usr/bin/env bash
 



cat > test/test_load/test_load_database.ini << EOM
[postgresql_test_load_database]
host=$PGHOST
port=5432
database=test_load_database
user=$USER
password=$PGPASSWORD
EOM

psql -c 'DROP DATABASE IF EXISTS test_load_database;'
psql -c 'CREATE DATABASE test_load_database;'
psql -f test/test_load/test_load_database.sql

pytest --log-cli-level=INFO test/test_load/test_load_data_handler.py -vvvrP

if  [[ $PGUSER!='postgres_user' ]]; then psql -c "DROP DATABASE IF EXISTS test_load_database"; fi