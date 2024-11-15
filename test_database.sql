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
