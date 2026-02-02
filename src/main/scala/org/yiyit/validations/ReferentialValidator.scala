package org.yiyit.validations

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object ReferentialValidator {
  def validate(df: DataFrame, reglas: Array[Row], jdbcUrl: String, connectionProps: Properties): List[String] = {
    val spark = df.sparkSession

    // Filtrar columnas con referencia definida
    val colsConReferencia = reglas.filter { r =>
      Option(r.getAs[String]("referential_bbdd")).exists(_.nonEmpty) &&
        Option(r.getAs[String]("referential_table_field_name")).exists(_.nonEmpty)
    }

    if (colsConReferencia.isEmpty) return List.empty

    colsConReferencia.flatMap { regla =>
      val colName = regla.getAs[String]("field_name")
      val refBbdd = regla.getAs[String]("referential_bbdd")
      val refTableField = regla.getAs[String]("referential_table_field_name")

      // Parsear tabla.columna
      val parts = refTableField.split("\\.")
      if (parts.length != 2) {
        List(s"[IR] Columna '$colName': Formato inválido en referential_table_field_name: $refTableField")
      } else {
        val refTable = s"$refBbdd.${parts(0)}"
        val refColumn = parts(1)

        validateColumn(df, colName, refTable, refColumn, spark, jdbcUrl, connectionProps)
      }
    }.toList
  }

  private def validateColumn(df: DataFrame, colName: String, refTable: String, refColumn: String,
                             spark: SparkSession, jdbcUrl: String, connectionProps: Properties): List[String] = {
    try {
      // Cargar valores de referencia
      val refDf = spark.read.jdbc(jdbcUrl, s"(SELECT DISTINCT $refColumn FROM $refTable) as ref", connectionProps)
      val refValues = refDf.select(col(refColumn).cast("string")).distinct()

      // Obtener valores distintos de la columna a validar
      val colValues = df.select(col(colName).cast("string")).filter(col(colName).isNotNull).distinct()

      // Encontrar valores que no existen en la referencia
      val invalidValues = colValues.join(refValues, colValues(colName) === refValues(refColumn), "left_anti")
      val invalidCount = invalidValues.count()

      if (invalidCount > 0) {
        List(s"[IR] Columna '$colName': $invalidCount valores no existen en $refTable.$refColumn")
      } else {
        List.empty
      }
    } catch {
      case e: Exception =>
        List(s"[IR] Columna '$colName': Error al validar contra $refTable - ${e.getMessage}")
    }
  }
}






















