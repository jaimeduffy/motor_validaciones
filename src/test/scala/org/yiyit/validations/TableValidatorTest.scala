package org.yiyit.validations

import org.scalatest.funsuite.AnyFunSuite
import org.yiyit.SparkSessionProvider
import org.apache.spark.sql.types._

class TableValidatorTest extends AnyFunSuite with SparkSessionProvider {
  import spark.implicits._

  test("estructura OK con header=true y columnas coincidentes") {
    // Arrange
    val df = Seq(
      ("T1", "Cover", "metric", "B3", "_c1", 1, "data_as_of"),
      ("T2", "CCY", "metric", "C4", "_c2", 2, "ccy1")
    ).toDF("template_code", "sheet", "data_type", "excel_cell", "column_x", "row_y", "data_name")
    val expected = Seq("template_code", "sheet", "data_type", "excel_cell", "column_x", "row_y", "data_name")

    // Act
    val result = TableValidator.validateStructure(df, expected, hasHeader = true)

    // Assert
    assert(result.success)
    assert(result.errorMessage.isEmpty)
    assert(result.dataFrame.isDefined)
  }

  test("estructura OK con header=false renombra columnas") {
    // Arrange: columnas que no coinciden con los nombres esperados
    val df = Seq(
      ("T1", "Cover", "metric", "B3", "_c1", 1, "data_as_of")
    ).toDF("_c0", "_c1", "_c2", "_c3", "_c4", "_c5", "_c6")
    val expected = Seq("template_code", "sheet", "data_type", "excel_cell", "column_x", "row_y", "data_name")

    // Act
    val result = TableValidator.validateStructure(df, expected, hasHeader = false)

    // Assert
    assert(result.success)
    assert(result.dataFrame.isDefined)
    val renamedCols = result.dataFrame.get.columns.toSet
    assert(renamedCols == expected.toSet)
  }

  test("fallo si tabla está vacía") {
    // Arrange
    val df = spark.createDataFrame(
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      StructType(Seq(
        StructField("a", StringType),
        StructField("b",StringType)
      ))
    )
    val expected = Seq("a", "b")

    // Act
    val result = TableValidator.validateStructure(df, expected, hasHeader = true)

    // Assert
    assert(!result.success)
    assert(result.errorMessage.get.contains("vacía"))
  }

  test("fallo si número de columnas no coincide") {
    // Arrange
    val df = Seq(("a", "b")).toDF("col1", "col2")
    val expected = Seq("col1", "col2", "col3") // esperamos 3, hay 2

    // Act
    val result = TableValidator.validateStructure(df, expected, hasHeader = true)

    // Assert
    assert(!result.success)
    assert(result.errorMessage.get.contains("Número de columnas incorrecto"))
  }

  test("fallo si header=true y nombres de columnas no coinciden") {
    // Arrange
    val df = Seq(("a", "b")).toDF("col1", "col_WRONG")
    val expected = Seq("col1", "col2")

    // Act
    val result = TableValidator.validateStructure(df, expected, hasHeader = true)

    // Assert
    assert(!result.success)
    assert(result.errorMessage.get.contains("Faltan") || result.errorMessage.get.contains("Sobran"))
  }
}
