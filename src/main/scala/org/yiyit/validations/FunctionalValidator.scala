package org.yiyit.validations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.yiyit.models.ValidationError

object FunctionalValidator {

  def validate(df: DataFrame): List[ValidationError] = {
    var errores = List.empty[ValidationError]

    // Validación 1: data_as_of, cristine_unit, excel_title exactamente 1 vez por (template_code, sheet)
    errores = errores ++ validacion1(df)

    // Validación 2: al menos 1 data_name que comience por "ccy" por (template_code, sheet)
    errores = errores ++ validacion2(df)

    // Validación 3: column_x debe comenzar por "_c"
    errores = errores ++ validacion3(df)

    // Validación 4: excel_cell debe estar dentro del rango válido de la tabla
    errores = errores ++ validacion4(df)

    errores
  }

  // Validación 1: una sola pasada con pivot en lugar de 3 groupBy separados
  private def validacion1(df: DataFrame): List[ValidationError] = {
    val requiredNames = List("data_as_of", "cristine_unit", "excel_title")

    // Filtramos solo los data_name que nos interesan y contamos por (template_code, sheet, data_name) en una sola pasada
    val counts = df.filter(col("data_name").isin(requiredNames: _*))
      .groupBy("template_code", "sheet")
      .pivot("data_name", requiredNames)
      .count()

    // Para cada nombre requerido, buscamos pares donde el count no sea exactamente 1
    requiredNames.flatMap { name =>
      // Si la columna no existe en el pivot, significa que ningún par tiene ese data_name
      val invalidCount = if (counts.columns.contains(name)) {
        counts.filter(col(name).isNull || col(name) =!= 1).count()
      } else {
        // No existe en ningún par -> todos los pares fallan
        counts.count()
      }

      if (invalidCount > 0) {
        Some(ValidationError(
          columnName = "data_name",
          errorMessage = s"[FUNC] data_name='$name': $invalidCount pares (template_code, sheet) no tienen exactamente 1 registro",
          errorType = Some("FUNCTIONAL_ERROR")
        ))
      } else None
    }
  }

  // Validación 2: al menos 1 data_name que comience por "ccy" por cada (template_code, sheet)
  private def validacion2(df: DataFrame): List[ValidationError] = {
    val totalPairs = df.select("template_code", "sheet").distinct().count()

    val pairsWithCcy = df.filter(lower(col("data_name")).startsWith("ccy"))
      .select("template_code", "sheet")
      .distinct()
      .count()

    val missing = totalPairs - pairsWithCcy
    if (missing > 0) {
      List(ValidationError(
        columnName = "data_name",
        errorMessage = s"[FUNC] data_name comenzando por 'ccy': $missing pares (template_code, sheet) sin registro",
        errorType = Some("FUNCTIONAL_ERROR")
      ))
    } else List.empty
  }

  // Validación 3: column_x debe comenzar por "_c"
  private def validacion3(df: DataFrame): List[ValidationError] = {
    val invalidCount = df.filter(
      col("column_x").isNotNull &&
        trim(col("column_x")) =!= "" &&
        !col("column_x").startsWith("_c")
    ).count()

    if (invalidCount > 0) {
      List(ValidationError(
        columnName = "column_x",
        errorMessage = s"[FUNC] Columna 'column_x': $invalidCount valores no comienzan por '_c'",
        errorType = Some("FUNCTIONAL_ERROR")
      ))
    } else List.empty
  }

  // Validación 4: excel_cell dentro del rango válido (ej: "B3" -> columna 2, fila 3)
  private def validacion4(df: DataFrame): List[ValidationError] = {
    val numCols = df.columns.length
    val numRows = df.count()

    // Convertir letras de columna Excel a número
    // Soporta hasta 2 letras
    val dfWithParsed = df.filter(col("excel_cell").isNotNull && trim(col("excel_cell")) =!= "")
      .withColumn("cell_col_letters", upper(regexp_extract(col("excel_cell"), "^([A-Za-z]+)", 1)))
      .withColumn("cell_col_len", length(col("cell_col_letters")))
      .withColumn("cell_col_num",
        when(col("cell_col_len") === 1,
          // Una letra: A=1, B=2, ..., Z=26
          ascii(col("cell_col_letters")) - lit(64)
        ).when(col("cell_col_len") === 2,
          // Dos letras
          (ascii(substring(col("cell_col_letters"), 1, 1)) - lit(64)) * lit(26) +
            (ascii(substring(col("cell_col_letters"), 2, 1)) - lit(64))
        ).otherwise(lit(0))
      )
      .withColumn("cell_row", regexp_extract(col("excel_cell"), "([0-9]+)$", 1).cast("long"))

    // Validar que columna y fila estén dentro del rango
    val invalidCells = dfWithParsed.filter(
      col("cell_col_num") > numCols || col("cell_col_num") < 1 ||
        col("cell_row") > numRows || col("cell_row") < 1
    ).count()

    if (invalidCells > 0) {
      List(ValidationError(
        columnName = "excel_cell",
        errorMessage = s"[FUNC] Columna 'excel_cell': $invalidCells valores fuera del rango válido (cols: $numCols, rows: $numRows)",
        errorType = Some("FUNCTIONAL_ERROR")
      ))
    } else List.empty
  }
}