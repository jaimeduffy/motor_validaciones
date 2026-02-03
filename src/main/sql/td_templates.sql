-- Crear el esquema
CREATE SCHEMA IF NOT EXISTS bu_yiyit_bigdata;

-- Crear la tabla
DROP TABLE IF EXISTS bu_yiyit_bigdata.td_templates;

CREATE TABLE bu_yiyit_bigdata.td_templates (
    source VARCHAR(50),
    template_code VARCHAR(50) PRIMARY KEY,
    template_text VARCHAR(50)
);

-- Insertar los datos del excel
INSERT INTO bu_yiyit_bigdata.td_templates (source, template_code, template_text) VALUES
('yiyit_bigdata', '1', 'FRC'),
('yiyit_bigdata', '2', 'ALCO'),
('yiyit_bigdata', '3', 'LCR'),
('yiyit_bigdata', '4', 'NSFR'),
('yiyit_bigdata', '6', 'STRESS'),
('yiyit_bigdata', '8', 'LIP'),
('yiyit_bigdata', '10', 'VaR'),
('yiyit_bigdata', '11', 'ALI'),
('yiyit_bigdata', '12', '-'),
('yiyit_bigdata', '15', 'SOT'),
('yiyit_bigdata', '16', 'DAILYLCR'),
('yiyit_bigdata', '17', 'INTRADIA'),
('yiyit_bigdata', '1_multi', 'FRC'),
('yiyit_bigdata', '15_multi', 'SOT');