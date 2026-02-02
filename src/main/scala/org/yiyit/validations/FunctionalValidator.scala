package org.yiyit.validations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FunctionalValidator {

  def validate(df: DataFrame): List[String] = {
    var errores = List.empty[String]

    // Validación 1: Para cada (template_code, sheet) debe existir exactamente 1 registro con data_name = data_as_of, cristine_unit, excel_title
    errores = errores ++ validateRequiredDataNames(df)

    // Validación 2: Para cada (template_code, sheet) debe existir al menos 1 registro con data_name que comience por "ccy"
    errores = errores ++ validateCcyExists(df)

    // Validación 3: column_x debe comenzar por "_c"
    errores = errores ++ validateColumnXFormat(df)

    // Validación 4: excel_cell debe estar dentro del rango válido
    errores = errores ++ validateExcelCellRange(df)

    errores
  }

  // Validación 1: data_as_of, cristine_unit, excel_title deben aparecer exactamente 1 vez por (template_code, sheet)
  private def validateRequiredDataNames(df: DataFrame): List[String] = {
    val requiredNames = List("data_as_of", "cristine_unit", "excel_title")

    requiredNames.flatMap { name =>
      val counts = df.filter(col("data_name") === name)
        .groupBy("template_code", "sheet")
        .count()
        .filter(col("count") =!= 1)
        .count()

      if (counts > 0) Some(s"[FUNC] data_name='$name': $counts pares (template_code, sheet) no tienen exactamente 1 registro")
      else None
    }
  }

  // Validación 2: Debe existir al menos 1 data_name que comience por "ccy" por cada (template_code, sheet)
  private def validateCcyExists(df: DataFrame): List[String] = {
    val totalPairs = df.select("template_code", "sheet").distinct().count()

    val pairsWithCcy = df.filter(lower(col("data_name")).startsWith("ccy"))
      .select("template_code", "sheet")
      .distinct()
      .count()

    val missing = totalPairs - pairsWithCcy
    if (missing > 0) List(s"[FUNC] data_name comenzando por 'ccy': $missing pares (template_code, sheet) sin registro")
    else List.empty
  }

  // Validación 3: column_x debe comenzar por "_c"
  private def validateColumnXFormat(df: DataFrame): List[String] = {
    val invalidCount = df.filter(
      col("column_x").isNotNull &&
        trim(col("column_x")) =!= "" &&
        !col("column_x").startsWith("_c")
    ).count()

    if (invalidCount > 0) List(s"[FUNC] Columna 'column_x': $invalidCount valores no comienzan por '_c'")
    else List.empty
  }

  // Validación 4: excel_cell debe estar dentro del rango válido (ej: "B3" -> columna 2, fila 3)
  private def validateExcelCellRange(df: DataFrame): List[String] = {
    val numCols = df.columns.length
    val numRows = df.count()

    // Extraer columna y fila de excel_cell (formato: A1, B3, AA10, etc.)
    val dfWithParsed = df.filter(col("excel_cell").isNotNull && trim(col("excel_cell")) =!= "")
      .withColumn("cell_col", regexp_extract(col("excel_cell"), "^([A-Za-z]+)", 1))
      .withColumn("cell_row", regexp_extract(col("excel_cell"), "([0-9]+)$", 1).cast("long"))

    // Convertir letra(s) de columna a número (A=1, B=2, ..., Z=26, AA=27, etc.)
    val invalidCells = dfWithParsed.filter(
      col("cell_row") > numRows || col("cell_row") < 1
    ).count()

    if (invalidCells > 0) List(s"[FUNC] Columna 'excel_cell': $invalidCells valores fuera del rango válido de filas")
    else List.empty
  }
}
