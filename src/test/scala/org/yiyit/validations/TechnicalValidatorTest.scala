package org.yiyit.validations

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.yiyit.SparkSessionProvider

class TechnicalValidatorTest extends AnyFunSuite with SparkSessionProvider {
  import spark.implicits._
  // =========================================================================
  // Helper: construir Array[Row] simulando reglas de semantic_layer
  // =========================================================================
  private def buildRule(
    fieldName: String,
    dataType: String = "STRING",
    nullable: Boolean = true,
    length: String = null,
    pk: Boolean = false
  ): Row = {
    Row(fieldName, dataType, nullable, length, pk)
  }

  private val ruleSchema = StructType(Seq(
    StructField("field_name", StringType),
    StructField("data_type", StringType),
    StructField("nullable", BooleanType),
    StructField("length", StringType),
    StructField("pk", BooleanType)
  ))

  private def toRulesArray(rules: Seq[Row]): Array[Row] = {
    rules.toArray
  }

  // =========================================================================
  // Tests: validateDataTypes
  // =========================================================================

  test("validateDataTypes - todos los INT son válidos") {
    // Arrange
    val df = Seq("1", "2", "-3").toDF("row_y")
    val reglas = toRulesArray(Seq(buildRule("row_y", "INT")))

    // Act
    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    // Assert
    assert(errores.isEmpty)
  }

  test("validateDataTypes - detecta INT inválido") {
    // Arrange
    val df = Seq("1", "abc", "3.5").toDF("row_y")
    val reglas = toRulesArray(Seq(buildRule("row_y", "INT")))

    // Act
    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    // Assert
    assert(errores.nonEmpty)
    assert(errores.head.columnName == "row_y")
    assert(errores.head.errorMessage.contains("2")) // 2 valores inválidos: "abc" y "3.5"
  }

  test("validateDataTypes - DECIMAL válidos") {
    // Arrange
    val df = Seq("1.5", "-2.0", "100").toDF("score")
    val reglas = toRulesArray(Seq(buildRule("score", "DECIMAL(10,2)")))

    // Act
    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    // Assert
    assert(errores.isEmpty)
  }

  test("validateDataTypes - detecta DECIMAL inválido") {
    // Arrange
    val df = Seq("1.5", "abc").toDF("score")
    val reglas = toRulesArray(Seq(buildRule("score", "DECIMAL(10,2)")))

    // Act
    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    // Assert
    assert(errores.nonEmpty)
    assert(errores.head.errorMessage.contains("1")) // 1 valor inválido
  }

  test("validateDataTypes - DATE válidas con formato yyyy/MM/dd") {
    // Arrange
    val df = Seq("2022/03/12", "2023/01/01").toDF("fecha")
    val reglas = toRulesArray(Seq(buildRule("fecha", "DATE(yyyy/MM/dd)")))

    // Act
    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    // Assert
    assert(errores.isEmpty)
  }

  test("validateDataTypes - detecta DATE con formato incorrecto") {
    // Arrange
    val df = Seq("2022/03/12", "12-03-2022").toDF("fecha")
    val reglas = toRulesArray(Seq(buildRule("fecha", "DATE(yyyy/MM/dd)")))

    // Act
    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    // Assert
    assert(errores.nonEmpty)
  }

  test("validateDataTypes - STRING nunca falla") {
    // Arrange
    val df = Seq("cualquier cosa", "123", "").toDF("nombre")
    val reglas = toRulesArray(Seq(buildRule("nombre", "STRING")))

    // Act
    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    // Assert
    assert(errores.isEmpty)
  }

  test("validateDataTypes - valores vacíos y nulos no se reportan como error de tipo") {
    // Arrange: los vacíos/nulos se validan en validateNulls, no aquí
    val df = Seq("1", "", null: String).toDF("row_y")
    val reglas = toRulesArray(Seq(buildRule("row_y", "INT")))

    // Act
    val errores = TechnicalValidator.validateDataTypes(df, reglas)

    // Assert
    assert(errores.isEmpty) // solo "1" es evaluado, "" y null se ignoran
  }

  // =========================================================================
  // Tests: validateNulls
  // =========================================================================

  test("validateNulls - sin nulos cuando nullable=false") {
    // Arrange
    val df = Seq(("T1", "_c1"), ("T2", "_c2")).toDF("template_code", "column_x")
    val reglas = toRulesArray(Seq(
      buildRule("template_code", nullable = false),
      buildRule("column_x", nullable = false)
    ))

    // Act
    val errores = TechnicalValidator.validateNulls(df, reglas)

    // Assert
    assert(errores.isEmpty)
  }

  test("validateNulls - detecta nulos cuando nullable=false") {
    // Arrange
    val df = Seq(("T1", null: String), (null: String, "_c2")).toDF("template_code", "column_x")
    val reglas = toRulesArray(Seq(
      buildRule("template_code", nullable = false),
      buildRule("column_x", nullable = false)
    ))

    // Act
    val errores = TechnicalValidator.validateNulls(df, reglas)

    // Assert
    assert(errores.length == 2)
  }

  test("validateNulls - detecta vacíos como nulos cuando nullable=false") {
    // Arrange
    val df = Seq("  ", "ok").toDF("template_code", "column_x")
    val reglas = toRulesArray(Seq(
      buildRule("template_code", nullable = false),
      buildRule("column_x", nullable = false)
    ))

    // Act
    val errores = TechnicalValidator.validateNulls(df, reglas)

    // Assert
    assert(errores.length == 1)
    assert(errores.head.columnName == "template_code")
  }

  test("validateNulls - ignora nulos cuando nullable=true") {
    // Arrange
    val df = Seq(null: String).toDF("data_type")
    val reglas = toRulesArray(Seq(buildRule("data_type", nullable = true)))

    // Act
    val errores = TechnicalValidator.validateNulls(df, reglas)

    // Assert
    assert(errores.isEmpty)
  }

  // =========================================================================
  // Tests: validateLengths
  // =========================================================================

  test("validateLengths - valores dentro del límite") {
    // Arrange
    val df = Seq("AB", "1234").toDF("col_a", "col_b")
    val reglas = toRulesArray(Seq(
      buildRule("col_a", length = "4"),
      buildRule("col_b", length = "4")
    ))

    // Act
    val errores = TechnicalValidator.validateLengths(df, reglas)

    // Assert
    assert(errores.isEmpty)
  }

  test("validateLengths - detecta valores que exceden longitud") {
    // Arrange
    val df = Seq("ABCDE", "AB").toDF("template_code")
    val reglas = toRulesArray(Seq(buildRule("template_code", length = "4")))

    // Act
    val errores = TechnicalValidator.validateLengths(df, reglas)

    // Assert
    assert(errores.nonEmpty)
    assert(errores.head.errorMessage.contains("1")) // 1 valor excede
  }

  test("validateLengths - sin validación si length es null") {
    // Arrange
    val df = Seq("un texto muy largo de prueba").toDF("campo")
    val reglas = toRulesArray(Seq(buildRule("campo", length = null)))

    // Act
    val errores = TechnicalValidator.validateLengths(df, reglas)

    // Assert
    assert(errores.isEmpty)
  }

  // =========================================================================
  // Tests: validatePrimaryKey
  // =========================================================================

  test("validatePrimaryKey - sin duplicados (PK compuesta)") {
    // Arrange: cada par (template_code, sheet) es único
    val df = Seq(
      ("2", "Cover", "metric"),
      ("2", "CCY", "metric"),
      ("3", "CCY", "metric")
    ).toDF("template_code", "sheet", "data_name")
    val pkColumns = Seq("template_code", "sheet")

    // Act
    val errores = TechnicalValidator.validatePrimaryKey(df, pkColumns)

    // Assert
    assert(errores.isEmpty)
  }

  test("validatePrimaryKey - detecta duplicados (PK compuesta)") {
    // Arrange: (2, Cover) aparece 2 veces
    val df = Seq(
      ("2", "Cover", "metric"),
      ("2", "Cover", "metric"),
      ("2", "CCY", "metric")
    ).toDF("template_code", "sheet", "data_name")
    val pkColumns = Seq("template_code", "sheet")

    // Act
    val errores = TechnicalValidator.validatePrimaryKey(df, pkColumns)

    // Assert
    assert(errores.nonEmpty)
    assert(errores.head.errorMessage.contains("1")) // 1 registro duplicado
    assert(errores.head.errorType.contains("PK_ERROR"))
  }

  test("validatePrimaryKey - sin validación si pkColumns está vacío") {
    // Arrange
    val df = Seq("a", "a").toDF("col1")

    // Act
    val errores = TechnicalValidator.validatePrimaryKey(df, Seq.empty)

    // Assert
    assert(errores.isEmpty)
  }

  test("validatePrimaryKey - PK simple sin duplicados") {
    // Arrange
    val df = Seq("1", "2", "3").toDF("id")

    // Act
    val errores = TechnicalValidator.validatePrimaryKey(df, Seq("id"))

    // Assert
    assert(errores.isEmpty)
  }
}
