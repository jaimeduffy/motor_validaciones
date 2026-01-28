package org.yiyit.validations

import org.apache.spark.sql.DataFrame

object TableValidator {

  case class Result(success: Boolean, errorMessage: Option[String], dataFrame: Option[DataFrame])

  def validateStructure(df: DataFrame, expectedColumns: Seq[String], hasHeader: Boolean): Result = {

    // Validar tabla vacía
    if (df.isEmpty) {
      return Result(success = false, Some("La tabla está vacía (0 filas)"), None)
    }

    // Validar número de columnas
    if (df.columns.length != expectedColumns.length) {
      return Result(success = false, Some(s"Número de columnas incorrecto. Esperadas: ${expectedColumns.length}, Encontradas: ${df.columns.length}"), None)
    }

    if (hasHeader) {
      // Validar nombres de columnas
      val reales = df.columns.map(_.toLowerCase).toSet
      val esperadas = expectedColumns.map(_.toLowerCase).toSet
      val faltantes = esperadas.diff(reales)
      val extras = reales.diff(esperadas)

      if (faltantes.nonEmpty || extras.nonEmpty) {
        val msg = (if (faltantes.nonEmpty) s"Faltan: ${faltantes.mkString(", ")}" else "") +
          (if (faltantes.nonEmpty && extras.nonEmpty) ". " else "") +
          (if (extras.nonEmpty) s"Sobran: ${extras.mkString(", ")}" else "")
        return Result(success = false, Some(msg), None)
      }
    }

    // Renombrar columnas según expectedColumns
    val renamed = df.columns.zip(expectedColumns).foldLeft(df) { case (d, (old, nuevo)) => d.withColumnRenamed(old, nuevo) }
    Result(success = true, None, Some(renamed))
  }
}