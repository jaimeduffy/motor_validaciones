package org.yiyit.validations

import org.apache.spark.sql.DataFrame
import scala.collection.Seq

object TableValidator {

  case class Result(success: Boolean, errorMessage: Option[String], dataFrame: Option[DataFrame])

  def validateStructure(df: DataFrame, expectedColumns: Seq[String], hasHeader: Boolean): Result = {

    val expected = expectedColumns.toList

    if (df.isEmpty) {
      return Result(success = false, Some("La tabla está vacía (0 filas)"), None)
    }

    if (df.columns.length != expected.length) {
      return Result(success = false, Some(s"Número de columnas incorrecto. Esperadas: ${expected.length}, Encontradas: ${df.columns.length}"), None)
    }

    if (hasHeader) {
      val reales = df.columns.map(_.toLowerCase).toSet
      val esperadas = expected.map(_.toLowerCase).toSet
      val faltantes = esperadas.diff(reales)
      val extras = reales.diff(esperadas)

      if (faltantes.nonEmpty || extras.nonEmpty) {
        val msg = (if (faltantes.nonEmpty) s"Faltan: ${faltantes.mkString(", ")}" else "") +
          (if (faltantes.nonEmpty && extras.nonEmpty) ". " else "") +
          (if (extras.nonEmpty) s"Sobran: ${extras.mkString(", ")}" else "")
        return Result(success = false, Some(msg), None)
      }
    }

    val renamed = df.columns.zip(expected).foldLeft(df) { case (d, (old, nuevo)) => d.withColumnRenamed(old, nuevo) }
    Result(success = true, None, Some(renamed))
  }
}