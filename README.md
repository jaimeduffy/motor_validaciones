# Motor de Validaciones Big Data

Motor de validación de datos para tablas PostgreSQL usando Apache Spark y Scala. Procesa tablas de gran volumen ejecutando validaciones técnicas, de integridad referencial y funcionales.

## Estructura del Proyecto

```
src/
├── main/
│   ├── resources/
│   │   ├── application.properties    # Configuración de conexión
│   │   └── log4j.properties          # Configuración de logs
│   └── scala/
│       └── org/
│           └── yiyit/
│               ├── App.scala                     # Orquestador principal
│               ├── utils/
│               │   └── DbLogger.scala            # Logger de errores a BD
│               └── validations/
│                   ├── FunctionalValidator.scala  # Reglas de negocio
│                   ├── ReferentialValidator.scala # Integridad referencial
│                   ├── TableValidator.scala       # Validación de estructura
│                   └── TechnicalValidator.scala   # Validaciones técnicas
└── sql/                                           # Scripts SQL
```

## Configuración

Editar `src/main/resources/application.properties` para configurar los parámetros de conexión con la base de datos:

```properties
jdbc.url=jdbc:postgresql://host:puerto/base_datos
jdbc.user=usuario
jdbc.password=contraseña
jdbc.driver=org.postgresql.Driver
```

## Base de Datos

### Tablas de Configuración

| Tabla | Descripción |
|-------|-------------|
| `table_configuration` | Configuración por tipo de tabla (header, ncols) |
| `semantic_layer` | Reglas de validación por columna |
| `trigger_control` | Control de ejecuciones y estado |
| `process_validation_logs` | Registro de errores |

## Flujo de Validación

```
┌─────────────┐
│  flag = 0   │ Pendiente
└──────┬──────┘
       ▼
┌─────────────┐
│  flag = 1   │ Ingesta OK
└──────┬──────┘
       ▼
┌─────────────┐
│  flag = 11  │ Procesando
└──────┬──────┘
       ▼
┌─────────────┐     ┌─────────────┐
│   FASE 1    │────▶│  flag = 31  │ Error estructura
│ Estructura  │     └─────────────┘
└──────┬──────┘
       ▼ OK (flag=12)
┌─────────────┐     ┌─────────────┐
│   FASE 2    │────▶│  flag = 32  │ Error técnico
│  Técnicas   │     └─────────────┘
└──────┬──────┘
       ▼ OK (flag=13)
┌─────────────┐     ┌─────────────┐
│   FASE 3    │────▶│  flag = 33  │ Error Integridad Referencial
│     IR      │     └─────────────┘
└──────┬──────┘
       ▼ OK (flag=14)
┌─────────────┐     ┌─────────────┐
│   FASE 4    │────▶│  flag = 34  │ Error funcional
│ Funcionales │     └─────────────┘
└──────┬──────┘
       ▼ OK
┌─────────────┐
│  flag = 2   │ Completado
└─────────────┘
```

## Validaciones

### Fase 1: Estructura (TableValidator)

- Valida número de columnas
- Si `header=true`: verifica nombres de columnas
- Si `header=false`: renombra columnas según `field_position`

### Fase 2: Técnicas (TechnicalValidator)

Ejecuta en orden secuencial (si una falla, no continúa):

1. **Tipos de datos**: STRING, INT, DECIMAL, DATE
2. **Nulos**: columnas con `nullable=false`
3. **Longitud**: máximo definido en `length`
4. **Primary Key**: unicidad de clave compuesta

### Fase 3: Integridad Referencial (ReferentialValidator)

Valida que valores existan en tabla de referencia cuando `referential_bbdd` y `referential_table_field_name` están definidos.

### Fase 4: Funcionales (FunctionalValidator)

- `data_as_of`, `cristine_unit`, `excel_title` exactamente 1 vez por (template_code, sheet)
- Al menos un `data_name` comenzando por "ccy" por cada par
- `column_x` debe comenzar por "_c"
- `excel_cell` dentro del rango válido

## Sistema de Flags

| Flag | Estado        | Descripción                  |
|------|---------------|------------------------------|
| 0    | Pendiente     | Esperando procesamiento      |
| 1    | Ingesta OK    | Carga de datos completada    |
| 11   | Procesando    | Validación en curso          |
| 12   | Estructura OK | Fase 1 completada            |
| 13   | Técnicas OK   | Fase 2 completada            |
| 14   | IR OK         | Fase 3 completada            |
| 2    | Completado    | Todas las validaciones OK    |
| 3    | Error sistema | Error de ejecución           |
| 31   | Error Fase 1  | Error en estructura          |
| 32   | Error Fase 2  | Error técnico                |
| 33   | Error Fase 3  | Error integridad referencial |
| 34   | Error Fase 4  | Error funcional              |


## Códigos de Error

| Código | Descripción |
|--------|-------------|
| STRUCTURE_MISMATCH | Error en estructura de columnas |
| DATA_TYPE_ERROR | Valor no cumple tipo de dato |
| NOT_NULL_ERROR | Valor nulo en columna obligatoria |
| LENGTH_ERROR | Valor excede longitud máxima |
| PK_ERROR | Duplicados en clave primaria |
| REF_INTEGRITY_ERROR | Valor no existe en tabla referencia |
| FUNCTIONAL_ERROR | Incumple regla de negocio |


## Uso

### 1. Insertar tarea en trigger_control

```sql
INSERT INTO trigger_control (id_type_table, table_name, tst_trigger_control, flag, row_count)
VALUES ('00007', 'schema.tabla_validacion', NOW(), 0, 0);
```

### 2. Ejecutar el motor, ejemplo de salida:

```
--> INICIANDO MOTOR DE VALIDACIÓN ...

========================================================
  PROCESANDO TRIGGER ID: 49
  Tabla: bu_tablas_madres_hist.tabla_validacion | id_type_table: 00007
========================================================
--> Configuración cargada: header = true
--> Flag actualizado a 1 (Ingesta OK)
--> Datos cargados: 10000000 registros
--> Flag actualizado a 11 (Procesando...)

--> [FASE 1] Validando estructura...
   OK
--> Flag actualizado a 12

--> [FASE 2.1] Validando tipos de datos...
   OK

--> [FASE 2.2] Validando nulos...
   OK

--> [FASE 2.3] Validando longitud...
   OK
--> Flag actualizado a 13

--> [FASE 3] Validando integridad referencial...
   OK
--> Flag actualizado a 14

--> [FASE 4] Validando reglas funcionales...
   OK

   VALIDACIÓN COMPLETADA - Flag actualizado a 2

--> Motor de validación finalizado.
```


