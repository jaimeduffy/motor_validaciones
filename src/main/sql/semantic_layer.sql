CREATE TABLE semantic_layer (
        id_type_table VARCHAR ( 5 ) NOT NULL,
        field_position INTEGER,
        field_name VARCHAR ( 255 )  NOT NULL,
        field_description VARCHAR ( 255 ),
        pk BOOLEAN,
        data_type VARCHAR ( 255 ),
        length VARCHAR ( 255 ),
        nullable BOOLEAN,
        decimal_symbol VARCHAR ( 255 ),
        referential_bbdd VARCHAR ( 255 ),
        referential_table_field_name VARCHAR ( 255 ),
        init_date TIMESTAMP,
        FOREIGN KEY (id_type_table) REFERENCES table_configuration(id_type_table),
        PRIMARY KEY (id_type_table, field_name)
)