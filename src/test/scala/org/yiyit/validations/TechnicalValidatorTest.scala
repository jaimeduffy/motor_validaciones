package org.yiyit.validations

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.yiyit.SparkSessionProvider

class TechnicalValidatorTest extends AnyFunSuite with SparkSessionProvider {
  import spark.implicits._
  // Esquema completo de semantic_layer
  private val semanticSchema = StructType(Seq(
    StructField("id_type_table", StringType),
    StructField("field_position", IntegerType),
    StructField("field_name", StringType),
    StructField("field_description", StringType),
    StructField("pk", BooleanType),
    StructField("data_type", StringType),
    StructField("length", StringType),
    StructField("nullable", BooleanType),
    StructField("decimal_symbol", StringType),
    StructField("referential_bbdd", StringType),
    StructField("referential_table_field_name", StringType),
    StructField("init_date", TimestampType)
  ))

  // Creamos un row con esquema
  private def buildRule(
                         fieldName: String,
                         dataType: String = "STRING",
                         nullable: Boolean = true,
                         length: String = null,
                         pk: Boolean = false
                       ): Row = {
    val row = Row(
      "T1",         // id_type_table
      null,          // field_position
      fieldName,     // field_name
      null,          // field_description
      pk,            // pk
      dataType,      // data_type
      length,        // length
      nullable,      // nullable
      null,          // decimal_symbol
      null,          // referential_bbdd
      null,          // referential_table_field_name
      null           // init_date
    )
    // Crear DataFrame con esquema y recoger el Row
    spark.createDataFrame(java.util.Collections.singletonList(row), semanticSchema).first()
  }

  private def toRulesArray(rules: Seq[Row]): Array[Row] = rules.toArray

  test("validateDataTypes - todos los INT son validos") {
    val df = Seq("1", "2", "-3").toDF("row_y")
    val reglas = toRulesArray(Seq(buildRule("row_y", "INT")))

    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    assert(errores.isEmpty)
  }

  test("validateDataTypes - detecta INT invalido") {
    val df = Seq("1", "abc", "3.5").toDF("row_y")
    val reglas = toRulesArray(Seq(buildRule("row_y", "INT")))

    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    assert(errores.nonEmpty)
    assert(errores.head.columnName == "row_y")
    assert(errores.head.errorMessage.contains("2"))
  }

  test("validateDataTypes - DECIMAL validos") {
    val df = Seq("1.5", "-2.0", "100").toDF("score")
    val reglas = toRulesArray(Seq(buildRule("score", "DECIMAL(10,2)")))

    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    assert(errores.isEmpty)
  }

  test("validateDataTypes - detecta DECIMAL invalido") {
    val df = Seq("1.5", "abc").toDF("score")
    val reglas = toRulesArray(Seq(buildRule("score", "DECIMAL(10,2)")))

    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    assert(errores.nonEmpty)
    assert(errores.head.errorMessage.contains("1")) // 1 valor invalido
  }

  test("validateDataTypes - DATE validas con formato yyyy/MM/dd") {
    val df = Seq("2022/03/12", "2023/01/01").toDF("fecha")
    val reglas = toRulesArray(Seq(buildRule("fecha", "DATE(yyyy/MM/dd)")))

    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    assert(errores.isEmpty)
  }

  test("validateDataTypes - detecta DATE con formato incorrecto") {
    val df = Seq("2022/03/12", "12-03-2022").toDF("fecha")
    val reglas = toRulesArray(Seq(buildRule("fecha", "DATE(yyyy/MM/dd)")))

    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    assert(errores.nonEmpty)
  }

  test("validateDataTypes - STRING nunca falla") {
    val df = Seq("cualquier cosa", "123", "").toDF("nombre")
    val reglas = toRulesArray(Seq(buildRule("nombre", "STRING")))

    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    assert(errores.isEmpty)
  }

  test("validateDataTypes - valores vacios y nulos no se reportan como error de tipo") {
    val df = Seq("1", "", null: String).toDF("row_y")
    val reglas = toRulesArray(Seq(buildRule("row_y", "INT")))

    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    assert(errores.isEmpty) // solo "1" es evaluado, "" y null se ignoran
  }

  // =========================================================================
  // Tests: validateNulls
  // =========================================================================

  test("validateNulls - sin nulos cuando nullable=false") {
    val df = Seq(("T1", "_c1"), ("T2", "_c2")).toDF("template_code", "column_x")
    val reglas = toRulesArray(Seq(
      buildRule("template_code", nullable = false),
      buildRule("column_x", nullable = false)
    ))

    val errores = TechnicalValidator.validateNulls(df, reglas)

    assert(errores.isEmpty)
  }

  test("validateNulls - detecta nulos cuando nullable=false") {
    val df = Seq(("T1", null: String), (null: String, "_c2")).toDF("template_code", "column_x")
    val reglas = toRulesArray(Seq(
      buildRule("template_code", nullable = false),
      buildRule("column_x", nullable = false)
    ))

    val errores = TechnicalValidator.validateNulls(df, reglas)

    assert(errores.length == 2)
  }

  test("validateNulls - detecta vacios como nulos cuando nullable=false") {
    val df = Seq(("  ", "ok")).toDF("template_code", "column_x")
    val reglas = toRulesArray(Seq(
      buildRule("template_code", nullable = false),
      buildRule("column_x", nullable = false)
    ))

    val errores = TechnicalValidator.validateNulls(df, reglas)

    assert(errores.length == 1)
    assert(errores.head.columnName == "template_code")
  }

  test("validateNulls - ignora nulos cuando nullable=true") {
    val df = Seq(null: String).toDF("data_type")
    val reglas = toRulesArray(Seq(buildRule("data_type", nullable = true)))

    val errores = TechnicalValidator.validateNulls(df, reglas)

    assert(errores.isEmpty)
  }

  // =========================================================================
  // Tests: validateLengths
  // =========================================================================

  test("validateLengths - valores dentro del limite") {
    val df = Seq(("AB", "1234")).toDF("col_a", "col_b")
    val reglas = toRulesArray(Seq(
      buildRule("col_a", length = "4"),
      buildRule("col_b", length = "4")
    ))

    val errores = TechnicalValidator.validateLengths(df, reglas)

    assert(errores.isEmpty)
  }

  test("validateLengths - detecta valores que exceden longitud") {
    val df = Seq("ABCDE", "AB").toDF("template_code")
    val reglas = toRulesArray(Seq(buildRule("template_code", length = "4")))

    val errores = TechnicalValidator.validateLengths(df, reglas)

    assert(errores.nonEmpty)
    assert(errores.head.errorMessage.contains("1")) // 1 valor excede
  }

  test("validateLengths - sin validacion si length es null") {
    val df = Seq("un texto muy largo de prueba").toDF("campo")
    val reglas = toRulesArray(Seq(buildRule("campo", length = null)))

    val errores = TechnicalValidator.validateLengths(df, reglas)

    assert(errores.isEmpty)
  }

  // =========================================================================
  // Tests: validatePrimaryKey
  // =========================================================================

  test("validatePrimaryKey - sin duplicados (PK compuesta)") {
    val df = Seq(
      ("2", "Cover", "metric"),
      ("2", "CCY", "metric"),
      ("3", "CCY", "metric")
    ).toDF("template_code", "sheet", "data_name")

    val errores = TechnicalValidator.validatePrimaryKey(df, Seq("template_code", "sheet"))

    assert(errores.isEmpty)
  }

  test("validatePrimaryKey - detecta duplicados (PK compuesta)") {
    val df = Seq(
      ("2", "Cover", "metric"),
      ("2", "Cover", "metric"),
      ("2", "CCY", "metric")
    ).toDF("template_code", "sheet", "data_name")

    val errores = TechnicalValidator.validatePrimaryKey(df, Seq("template_code", "sheet"))

    assert(errores.nonEmpty)
    assert(errores.head.errorMessage.contains("1"))
    assert(errores.head.errorType.contains("PK_ERROR"))
  }

  test("validatePrimaryKey - sin validacion si pkColumns esta vacio") {
    val df = Seq("a", "a").toDF("col1")

    val errores = TechnicalValidator.validatePrimaryKey(df, Seq.empty)

    assert(errores.isEmpty)
  }

  test("validatePrimaryKey - PK simple sin duplicados") {
    val df = Seq("1", "2", "3").toDF("id")

    val errores = TechnicalValidator.validatePrimaryKey(df, Seq("id"))

    assert(errores.isEmpty)
  }
}
