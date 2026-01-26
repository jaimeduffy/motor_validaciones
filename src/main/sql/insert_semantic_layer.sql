INSERT INTO public.semantic_layer 
(
    id_type_table, field_position, field_name, field_description, pk, 
    data_type, length, nullable, 
    referential_bbdd, referential_table_field_name, decimal_symbol,
    init_date
)
SELECT 
    t.id_table,      
    d.pos,           
    d.name,          
    d.field_desc,          
    False,         
    d.dtype,         
    d.len,           
    d.is_nullable::boolean,   
    d.ref_bbdd,
    d.ref_field,
    NULL,
    CURRENT_DATE
FROM 
    (VALUES
        (1, 'template_code', 'tipo de calculadora', 'STRING', 32, 'false', 'bu_yiyit_bigdata', 'td_templates.template_code'),
        (2, 'sheet', 'nombre de la pestaña', 'STRING', 255, 'false', NULL, NULL),
        (3, 'data_type', 'tipo de dato', 'STRING', 32, 'true', NULL, NULL),
        (4, 'excel_cell', 'celda donde se encuentra', 'STRING', 4, 'true', NULL, NULL),
        (5, 'column_x', 'columna donde se encuentra', 'STRING', 4, 'false', NULL, NULL),
        (6, 'row_y', 'fila donde se encuentra', 'INT', 3, 'false', NULL, NULL),
        (7, 'data_name', 'nombre del dato para usuario', 'STRING', 32, 'false', NULL, NULL),
        (8, 'idexcel', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (9, 'excname', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (10, 'coduni', 'output field', 'STRING', 255, 'false', NULL, NULL),
        (11, 'date_data', 'output field', 'STRING', 255, 'false', NULL, NULL),
        (12, 'axis', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (13, 'status', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (14, 'idarea', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (15, 'idcurr', 'output field', 'STRING', 255, 'false', NULL, NULL),
        (16, 'idsover', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (17, 'idprod', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (18, 'idisco', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (19, 'idbsi', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (20, 'idsegm', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (21, 'idasli', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (22, 'idtmbuc', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (23, 'idscene', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (24, 'idencum', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (25, 'idisin', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (26, 'idmatur', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (27, 'idauxdi', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (28, 'auvame', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (29, 'curve', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (30, 'idmetr', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (31, 'version', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (32, 'load_date', 'output field', 'STRING', 255, 'false', NULL, NULL),
        (33, 'tip_act', 'output field', 'STRING', 255, 'true', NULL, NULL),
        (34, 'car_aux', 'output field', 'STRING', 255, 'true', NULL, NULL)
    ) AS d(pos, name, field_desc, dtype, len, is_nullable, ref_bbdd, ref_field)
CROSS JOIN 
    (VALUES ('00001'), ('00002'), ('00003'), ('00004'), ('00005'), ('00006'), ('00007')) AS t(id_table);