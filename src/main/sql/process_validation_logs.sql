CREATE TABLE process_validation_logs (
        id_trigger INTEGER NOT NULL,
        validation_id VARCHAR(255) NOT NULL,
        type_validation VARCHAR(255) NOT NULL,
        table_name VARCHAR(255) NOT NULL,
        validation_msg VARCHAR(255),
        field_name VARCHAR(255),
        incidences VARCHAR(255) NOT NULL,
        flag INTEGER NOT NULL,
        execution_timestamp TIMESTAMP NOT NULL,
        FOREIGN KEY (id_trigger) REFERENCES trigger_control(id_trigger)
)