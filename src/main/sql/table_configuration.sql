CREATE TABLE table_configuration (
        id_type_table VARCHAR ( 5 ) PRIMARY KEY,
        type_table_name VARCHAR ( 255 ) NOT NULL,
        header BOOLEAN,
        user_id VARCHAR ( 50 ) NOT NULL,
        nCols INT NOT NULL
);