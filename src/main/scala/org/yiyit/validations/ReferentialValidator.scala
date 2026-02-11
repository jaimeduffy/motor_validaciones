package org.yiyit.validations

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties
import org.yiyit.models.ValidationError

object ReferentialValidator {
  def validate(df: DataFrame, reglas: Array[Row], jdbcUrl: String, connectionProps: Properties): List[ValidationError] = {
    val spark = df.sparkSession

    // Filtrar columnas con referencia definida
    val colsConReferencia = reglas.filter { r =>
      Option(r.getAs[String]("referential_bbdd")).exists(_.nonEmpty) &&
        Option(r.getAs[String]("referential_table_field_name")).exists(_.nonEmpty)
    }

    if (colsConReferencia.isEmpty) return List.empty

    // Separar reglas válidas de las que tienen formato inválido en referential_table_field_name
    val (reglasValidas, reglasInvalidas) = colsConReferencia.partition { regla =>
      regla.getAs[String]("referential_table_field_name").split("\\.").length == 2
    }

    // Errores de formato
    val formatErrors = reglasInvalidas.map { regla =>
      val colName = regla.getAs[String]("field_name")
      val refTableField = regla.getAs[String]("referential_table_field_name")
      ValidationError(
        columnName = colName,
        errorMessage = s"[IR] Columna '$colName': Formato inválido en referential_table_field_name: $refTableField",
        errorType = Some("REF_INTEGRITY_ERROR")
      )
    }.toList

    // Agrupamos por (tabla, columna) de referencia para hacer una sola lectura JDBC por tabla
    val reglasAgrupadas = reglasValidas.map { regla =>
      val colName = regla.getAs[String]("field_name")
      val refBbdd = regla.getAs[String]("referential_bbdd")
      val parts = regla.getAs[String]("referential_table_field_name").split("\\.")
      (s"$refBbdd.${parts(0)}", parts(1), colName)
    }.groupBy { case (refTable, refColumn, _) => (refTable, refColumn) }

    // Validamos: una lectura JDBC por cada (tabla, columna) de referencia
    val validationErrors = reglasAgrupadas.flatMap { case ((refTable, refColumn), entries) =>
      try {
        // Cargamos valores de referencia una sola vez
        val refDf = spark.read.jdbc(jdbcUrl, s"(SELECT DISTINCT $refColumn FROM $refTable) as ref", connectionProps)
        val refValues = refDf.select(col(refColumn).cast("string")).distinct()

        entries.flatMap { case (_, _, colName) =>
          // Valores distintos de la columna a validar
          val colValues = df.select(col(colName).cast("string")).filter(col(colName).isNotNull).distinct()
          // left_anti: valores que no existen en la referencia
          val invalidCount = colValues.join(refValues, colValues(colName) === refValues(refColumn), "left_anti").count()

          if (invalidCount > 0) {
            Some(ValidationError(
              columnName = colName,
              errorMessage = s"[IR] Columna '$colName': $invalidCount valores no existen en $refTable.$refColumn",
              errorType = Some("REF_INTEGRITY_ERROR")
            ))
          } else None
        }
      } catch {
        case e: Exception =>
          entries.map { case (_, _, colName) =>
            ValidationError(
              columnName = colName,
              errorMessage = s"[IR] Columna '$colName': Error al validar contra $refTable - ${e.getMessage}",
              errorType = Some("REF_INTEGRITY_ERROR")
            )
          }
      }
    }.toList

    formatErrors ++ validationErrors
  }
}