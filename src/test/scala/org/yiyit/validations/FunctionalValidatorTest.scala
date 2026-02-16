package org.yiyit.validations

import org.scalatest.funsuite.AnyFunSuite
import org.yiyit.SparkSessionProvider

class FunctionalValidatorTest extends AnyFunSuite with SparkSessionProvider {
  import spark.implicits._
  // =========================================================================
  // Helper: crear DF con la estructura de la tabla madre
  // =========================================================================
  private def makeDF(rows: Seq[(String, String, String, String, String, Int, String)]) = {
    rows.toDF("template_code", "sheet", "data_type", "excel_cell", "column_x", "row_y", "data_name")
  }

  // =========================================================================
  // Tests: validacion1 - data_as_of, cristine_unit, excel_title exactamente 1 por (template_code, sheet)
  // =========================================================================

  test("validacion1 - OK: cada par tiene exactamente 1 data_as_of, cristine_unit, excel_title") {
    // Arrange
    val df = makeDF(Seq(
      ("T1", "Cover", "STRING", "A1", "_c1", 1, "data_as_of"),
      ("T1", "Cover", "STRING", "A2", "_c1", 2, "cristine_unit"),
      ("T1", "Cover", "STRING", "A3", "_c1", 3, "excel_title"),
      ("T1", "Cover", "STRING", "A4", "_c1", 4, "ccy1"),
      ("T1", "Cover", "STRING", "A5", "_c1", 5, "metric")
    ))

    // Act
    val errores = FunctionalValidator.validate(df)

    // Assert: no debería haber errores de validacion1
    // CORRECTO (Le damos nombre 'e' y lo usamos varias veces)
    val erroresV1 = errores.filter(e =>
      e.errorMessage.contains("data_name='data_as_of'") ||
        e.errorMessage.contains("data_name='cristine_unit'") ||
        e.errorMessage.contains("data_name='excel_title'")
    )
    assert(erroresV1.isEmpty)
  }

  test("validacion1 - FALLO: falta data_as_of en un par") {
    // Arrange: no hay data_as_of
    val df = makeDF(Seq(
      ("T1", "Cover", "STRING", "A1", "_c1", 1, "cristine_unit"),
      ("T1", "Cover", "STRING", "A2", "_c1", 2, "excel_title"),
      ("T1", "Cover", "STRING", "A3", "_c1", 3, "ccy1")
    ))

    // Act
    val errores = FunctionalValidator.validate(df)

    // Assert
    val erroresDataAsOf = errores.filter(_.errorMessage.contains("data_as_of"))
    assert(erroresDataAsOf.nonEmpty)
  }

  test("validacion1 - FALLO: data_as_of duplicado en un par") {
    // Arrange: 2 data_as_of para el mismo par
    val df = makeDF(Seq(
      ("T1", "Cover", "STRING", "A1", "_c1", 1, "data_as_of"),
      ("T1", "Cover", "STRING", "A2", "_c1", 2, "data_as_of"),
      ("T1", "Cover", "STRING", "A3", "_c1", 3, "cristine_unit"),
      ("T1", "Cover", "STRING", "A4", "_c1", 4, "excel_title"),
      ("T1", "Cover", "STRING", "A5", "_c1", 5, "ccy1")
    ))

    // Act
    val errores = FunctionalValidator.validate(df)

    // Assert
    val erroresDataAsOf = errores.filter(_.errorMessage.contains("data_as_of"))
    assert(erroresDataAsOf.nonEmpty)
  }

  // =========================================================================
  // Tests: validacion2 - al menos 1 data_name que comience por "ccy"
  // =========================================================================

  test("validacion2 - OK: existe data_name que comienza por ccy") {
    // Arrange
    val df = makeDF(Seq(
      ("T1", "Cover", "STRING", "A1", "_c1", 1, "data_as_of"),
      ("T1", "Cover", "STRING", "A2", "_c1", 2, "cristine_unit"),
      ("T1", "Cover", "STRING", "A3", "_c1", 3, "excel_title"),
      ("T1", "Cover", "STRING", "A4", "_c1", 4, "ccy1")
    ))

    // Act
    val errores = FunctionalValidator.validate(df)

    // Assert
    val erroresCcy = errores.filter(_.errorMessage.contains("ccy"))
    assert(erroresCcy.isEmpty)
  }

  test("validacion2 - FALLO: ningún data_name comienza por ccy") {
    // Arrange
    val df = makeDF(Seq(
      ("T1", "Cover", "STRING", "A1", "_c1", 1, "data_as_of"),
      ("T1", "Cover", "STRING", "A2", "_c1", 2, "cristine_unit"),
      ("T1", "Cover", "STRING", "A3", "_c1", 3, "excel_title"),
      ("T1", "Cover", "STRING", "A4", "_c1", 4, "metric")
    ))

    // Act
    val errores = FunctionalValidator.validate(df)

    // Assert
    val erroresCcy = errores.filter(_.errorMessage.contains("ccy"))
    assert(erroresCcy.nonEmpty)
  }

  // =========================================================================
  // Tests: validacion3 - column_x debe comenzar por "_c"
  // =========================================================================

  test("validacion3 - OK: todos los column_x comienzan por _c") {
    // Arrange
    val df = makeDF(Seq(
      ("T1", "Cover", "STRING", "A1", "_c1", 1, "data_as_of"),
      ("T1", "Cover", "STRING", "A2", "_c2", 2, "cristine_unit"),
      ("T1", "Cover", "STRING", "A3", "_c10", 3, "excel_title"),
      ("T1", "Cover", "STRING", "A4", "_c1", 4, "ccy1")
    ))

    // Act
    val errores = FunctionalValidator.validate(df)

    // Assert
    val erroresColX = errores.filter(_.columnName == "column_x")
    assert(erroresColX.isEmpty)
  }

  test("validacion3 - FALLO: column_x no comienza por _c") {
    // Arrange
    val df = makeDF(Seq(
      ("T1", "Cover", "STRING", "A1", "_c1", 1, "data_as_of"),
      ("T1", "Cover", "STRING", "A2", "XX5", 2, "cristine_unit"),
      ("T1", "Cover", "STRING", "A3", "_c1", 3, "excel_title"),
      ("T1", "Cover", "STRING", "A4", "_c1", 4, "ccy1")
    ))

    // Act
    val errores = FunctionalValidator.validate(df)

    // Assert
    val erroresColX = errores.filter(_.columnName == "column_x")
    assert(erroresColX.nonEmpty)
    assert(erroresColX.head.errorMessage.contains("1")) // 1 valor inválido
  }

  // =========================================================================
  // Tests: validacion4 - excel_cell dentro del rango válido
  // =========================================================================

  test("validacion4 - OK: excel_cell dentro del rango") {
    // Arrange: 7 columnas, 4 filas -> B3 = columna 2, fila 3 -> válido
    val df = makeDF(Seq(
      ("T1", "Cover", "STRING", "A1", "_c1", 1, "data_as_of"),
      ("T1", "Cover", "STRING", "B3", "_c1", 2, "cristine_unit"),
      ("T1", "Cover", "STRING", "A2", "_c1", 3, "excel_title"),
      ("T1", "Cover", "STRING", "A1", "_c1", 4, "ccy1")
    ))

    // Act
    val errores = FunctionalValidator.validate(df)

    // Assert
    val erroresCell = errores.filter(_.columnName == "excel_cell")
    assert(erroresCell.isEmpty)
  }

  test("validacion4 - FALLO: excel_cell fuera del rango") {
    // Arrange: 7 columnas, 2 filas -> Z99 está fuera de rango
    val df = makeDF(Seq(
      ("T1", "Cover", "STRING", "Z99", "_c1", 1, "data_as_of"),
      ("T1", "Cover", "STRING", "A1", "_c1", 2, "cristine_unit"),
      ("T1", "Cover", "STRING", "A1", "_c1", 3, "excel_title"),
      ("T1", "Cover", "STRING", "A1", "_c1", 4, "ccy1")
    ))

    // Act
    val errores = FunctionalValidator.validate(df)

    // Assert
    val erroresCell = errores.filter(_.columnName == "excel_cell")
    assert(erroresCell.nonEmpty)
  }

  // =========================================================================
  // Tests: validate (integración de las 4 validaciones)
  // =========================================================================

  test("validate - dataset completamente válido sin errores") {
    // Arrange: dataset que cumple las 4 validaciones
    val df = makeDF(Seq(
      ("T1", "Cover", "STRING", "A1", "_c1", 1, "data_as_of"),
      ("T1", "Cover", "STRING", "A2", "_c2", 2, "cristine_unit"),
      ("T1", "Cover", "STRING", "A3", "_c3", 3, "excel_title"),
      ("T1", "Cover", "STRING", "A4", "_c1", 4, "ccy1"),
      ("T1", "Cover", "STRING", "B1", "_c2", 5, "metric")
    ))

    // Act
    val errores = FunctionalValidator.validate(df)

    // Assert
    assert(errores.isEmpty)
  }

  test("validate - múltiples pares (template_code, sheet) válidos") {
    // Arrange
    val df = makeDF(Seq(
      // Par 1: (T1, Cover)
      ("T1", "Cover", "STRING", "A1", "_c1", 1, "data_as_of"),
      ("T1", "Cover", "STRING", "A2", "_c2", 2, "cristine_unit"),
      ("T1", "Cover", "STRING", "A3", "_c3", 3, "excel_title"),
      ("T1", "Cover", "STRING", "A4", "_c1", 4, "ccy1"),
      // Par 2: (T1, CCY)
      ("T1", "CCY", "STRING", "A1", "_c1", 5, "data_as_of"),
      ("T1", "CCY", "STRING", "A2", "_c2", 6, "cristine_unit"),
      ("T1", "CCY", "STRING", "A3", "_c3", 7, "excel_title"),
      ("T1", "CCY", "STRING", "A4", "_c1", 8, "ccy2")
    ))

    // Act
    val errores = FunctionalValidator.validate(df)

    // Assert
    assert(errores.isEmpty)
  }
}
