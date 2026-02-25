# Motor de Validaciones Big Data

Motor de validación de datos para tablas PostgreSQL usando Apache Spark y Scala. Procesa tablas de gran volumen ejecutando validaciones de estructura, técnicas, de integridad referencial y funcionales.

## Estructura del Proyecto

```
src/
├── main/
│   ├── resources/
│   │   ├── application.properties    # Configuración de conexión JDBC
│   │   └── log4j.properties          # Configuración de logs
│   └── scala/
│       └── org/
│           └── yiyit/
│               ├── App.scala                     # Orquestador principal
│               ├── models/
│               │   └── ValidationError.scala     # Case class para errores
│               ├── utils/
│               │   └── DbLogger.scala            # Logger de errores a BD
│               └── validations/
│                   ├── TableValidator.scala       # Fase 1: Validación de estructura
│                   ├── TechnicalValidator.scala   # Fase 2: Validaciones técnicas
│                   ├── ReferentialValidator.scala # Fase 3: Integridad referencial
│                   └── FunctionalValidator.scala  # Fase 4: Reglas de negocio
├── test/
│   └── scala/
│       └── org/
│           └── yiyit/
│               ├── SparkSessionProvider.scala         # Trait con SparkSession compartida
│               ├── TableValidatorTest.scala            # 5 tests
│               ├── TechnicalValidatorTest.scala        # 19 tests
│               ├── ReferentialValidatorTest.scala      # 6 tests
│               └── FunctionalValidatorTest.scala       # 11 tests
└── sql/                                               # Scripts SQL de creación de tablas
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
| `table_configuration` | Configuración por tipo de tabla (header, extensión, separador) |
| `semantic_layer` | Reglas de validación por columna (tipo, nullable, PK, longitud, referencia) |
| `trigger_control` | Control de ejecuciones, estado (flags) y row_count |
| `process_validation_logs` | Registro de errores de validación |

## Flujo de Validación

```
┌─────────────┐
│  flag = 0   │ Pendiente
└──────┬──────┘
       ▼
┌─────────────┐
│  flag = 1   │ Ingesta OK (se registra row_count)
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
│   FASE 3    │────▶│  flag = 33  │ Error integridad referencial
│ Integridad  │     └─────────────┘
│ Referencial │
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

Si una fase falla, las siguientes no se ejecutan. Los errores se registran en `process_validation_logs`.

## Validaciones

### Fase 1: Estructura (TableValidator)

- Si la tabla está vacía → error inmediato
- Valida número de columnas contra `semantic_layer`
- Si `header=true`: verifica que los nombres de columnas coincidan exactamente
- Si `header=false`: la validación pasa, pero renombra las columnas con los valores de `field_name`
- Siempre renombra las columnas del DataFrame con los nombres de `semantic_layer`

### Fase 2: Técnicas (TechnicalValidator)

Ejecuta 4 sub-validaciones en secuencia con parada temprana (si una falla, no continúa):

1. **Tipos de datos**: STRING (sin validación), INT (regex `^-?[0-9]+$`), DECIMAL (regex), DATE (`to_date` con formato)
2. **Nulos**: columnas con `nullable=false` no admiten null ni vacío
3. **Longitud**: valores no exceden el máximo definido en `length`
4. **Primary Key**: combinación de columnas con `pk=true` debe ser única

Optimización: todos los checks de una misma sub-validación se ejecutan en un solo `select` con `sum(when(...))`, recorriendo el DataFrame una única vez.

### Fase 3: Integridad Referencial (ReferentialValidator)

Valida que los valores de una columna existan en la tabla de referencia cuando `referential_bbdd` y `referential_table_field_name` están definidos en `semantic_layer`.

1. Filtra reglas con ambos campos de referencia con valor
2. Valida formato `tabla.columna` (con punto separador)
3. Agrupa reglas por (tabla, columna) de referencia → una sola lectura JDBC por tabla
4. Carga valores de referencia con `SELECT DISTINCT`
5. Detecta valores inválidos con `left_anti` join

### Fase 4: Funcionales (FunctionalValidator)

Reglas de negocio específicas:

- Cada par `(template_code, sheet)` tiene exactamente 1 registro de `data_as_of`, `cristine_unit` y `excel_title`
- Cada par `(template_code, sheet)` tiene al menos 1 `data_name` que comience por "ccy"
- Los valores de `column_x` deben comenzar por "_c"
- `excel_cell` debe estar dentro del rango válido de la tabla (columna y fila dentro de las dimensiones)

## Sistema de Flags

| Flag | Estado | Descripción |
|------|--------|-------------|
| 0 | Pendiente | Esperando procesamiento |
| 1 | Ingesta OK | Datos cargados + row_count registrado |
| 11 | Procesando | Validación en curso |
| 12 | Estructura OK | Fase 1 completada |
| 13 | Técnicas OK | Fase 2 completada |
| 14 | IR OK | Fase 3 completada |
| 2 | Completado | Todas las validaciones OK |
| 3 | Error sistema | Error de ejecución no controlado |
| 31 | Error Fase 1 | Error en estructura |
| 32 | Error Fase 2 | Error técnico |
| 33 | Error Fase 3 | Error integridad referencial |
| 34 | Error Fase 4 | Error funcional |

## Códigos de Error

| Código | Descripción |
|--------|-------------|
| `STRUCTURE_MISMATCH` | Error en estructura de columnas |
| `DATA_TYPE_ERROR` | Valor no cumple tipo de dato |
| `NOT_NULL_ERROR` | Valor nulo en columna obligatoria |
| `LENGTH_ERROR` | Valor excede longitud máxima |
| `PK_ERROR` | Duplicados en clave primaria |
| `REF_INTEGRITY_ERROR` | Valor no existe en tabla de referencia |
| `FUNCTIONAL_ERROR` | Incumple regla de negocio |
| `EXECUTION_ERROR` | Error de sistema durante ejecución |

## Optimizaciones

| Optimización | Descripción |
|-------------|-------------|
| Lectura JDBC por ctid | 12 particiones por rangos de páginas físicas → TID Scan en PostgreSQL |
| Persistencia en memoria | `persist(MEMORY_ONLY)` + `count()` para carga inmediata y evitar relecturas |
| Reutilización de conexión | Una única conexión JDBC por trigger para todos los updates de flag |
| Fetchsize | `fetchsize=10000` para reducir round-trips JDBC |
| Select único en técnicas | Todos los checks por sub-validación en un solo `select` con `sum(when(...))` |
| Pivot en funcionales | Un `groupBy + pivot + count` en vez de 3 `groupBy` separados |
| Agrupación en IR | Reglas agrupadas por tabla de referencia → una lectura JDBC por tabla |

## Testing

41 tests unitarios con ScalaTest, patrón Arrange-Act-Assert:

| Módulo | Tests | Cobertura |
|--------|-------|-----------|
| `TableValidatorTest` | 5 | Header true/false, tabla vacía, columnas incorrectas, nombres distintos |
| `TechnicalValidatorTest` | 19 | Int, Decimal, Date, String, nulos, espacios, longitud, PK simple/compuesta |
| `ReferentialValidatorTest` | 6 | Sin referencia (null/vacío), formato inválido, H2 en memoria OK/KO, error conexión |
| `FunctionalValidatorTest` | 11 | data_name, ccy, column_x, excel_cell, integración |

- `SparkSessionProvider`: Trait con SparkSession local compartida (`@transient lazy val`)
- DataFrames de test construidos en memoria con `toDF()`
- Integridad referencial testeada con **H2 en memoria** (sin depender de PostgreSQL)

## Uso

### 1. Insertar tarea en trigger_control

```sql
INSERT INTO trigger_control (id_type_table, table_name, tst_trigger_control, flag, row_count)
VALUES ('00007', 'schema.tabla_validacion', NOW(), 0, 0);
```

### 2. Ejecutar el motor

Ejemplo de salida:

```
--> Iniciando motor de validación ...

========================================================
  PROCESANDO TRIGGER ID: 49
  Tabla: bu_tablas_madres_hist.tabla_validacion | id_type_table: 00007
========================================================

--> table_configuration cargado: header = true

--> Leyendo y cacheando los datos de la tabla madre...
    Páginas totales: 8334, ~695 páginas por partición

--> Flag actualizado a 1 (Ingesta OK) - 10000000 registros

--> Flag actualizado a 11 (Procesando...)

--> [FASE 1] Validando estructura...
   OK
   Flag actualizado a 12

--> [FASE 2.1] Validando tipos de datos...
   OK
--> [FASE 2.2] Validando nulos...
   OK
--> [FASE 2.3] Validando longitud...
   OK
--> [FASE 2.4] Validando PK (template_code, sheet)...
   OK
   Flag actualizado a 13

--> [FASE 3] Validaciones de Integridad Referencial...
   OK
   Flag actualizado a 14

--> [FASE 4] Validaciones funcionales...
   OK

--> Validación Completada - Flag actualizado a 2

--> Tiempo de procesamiento: 12.45 segundos

--> Motor de validación finalizado.
```