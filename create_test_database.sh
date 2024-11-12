#!/usr/bin/env bash

for file in "./test_database"/*.sql; do
    psql -f "${file}" > ${file%.sql}.txt
done