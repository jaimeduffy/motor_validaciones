CREATE TABLE trigger_control (
        id_trigger SERIAL PRIMARY KEY,
        id_type_table VARCHAR ( 5 ) NOT NULL,
        table_name VARCHAR(255) NOT NULL,
        tst_trigger_control TIMESTAMP NOT NULL,
        flag INTEGER,
        row_count VARCHAR(255), 
        nCols INTEGER,
        FOREIGN KEY (id_type_table) REFERENCES table_configuration(id_type_table)
)