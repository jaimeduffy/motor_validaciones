package org.yiyit.validations

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.yiyit.models.ValidationError
import scala.collection.Seq

object TechnicalValidator {

  // Valida que los valores cumplan el tipo indicado en data_type de semantic_layer
  def validateDataTypes(df: DataFrame, reglas: Array[Row]): List[ValidationError] = {
    val checks = reglas.flatMap { r =>
      val colName = r.getAs[String]("field_name")
      val rawType = Option(r.getAs[String]("data_type")).getOrElse("STRING")
      buildTypeCheck(colName, rawType)
    }

    if (checks.isEmpty) return List.empty

    // Un solo select para validar todos los tipos de datos a la vez
    val result = df.select(checks.map(_._2): _*).first()
    checks.zipWithIndex.collect {
      case ((colName, _, dataType), idx) if result.getLong(idx) > 0 =>
        ValidationError(
          columnName = colName,
          errorMessage = s"[TIPO] Columna '$colName': ${result.getLong(idx)} valores no son $dataType válidos",
          errorType = Some("DATA_TYPE_ERROR")
        )
    }.toList
  }

  // Construye el check de tipo para una columna según su dataType
  private def buildTypeCheck(colName: String, rawDataType: String): Option[(String, org.apache.spark.sql.Column, String)] = {
    val c = col(colName)
    val notEmpty = c.isNotNull && trim(c) =!= ""
    val dataTypeUpper = rawDataType.trim.toUpperCase

    dataTypeUpper match {
      case "STRING" => None
      case "INT" =>
        Some((colName, sum(when(notEmpty && !c.rlike("^-?[0-9]+$"), 1).otherwise(0)), "INT"))
      case d if d.startsWith("DECIMAL") =>
        Some((colName, sum(when(notEmpty && !c.rlike("^-?[0-9]+(\\.[0-9]+)?$"), 1).otherwise(0)), "DECIMAL"))
      case d if d.startsWith("DATE") =>
        // Extraer el formato del tipo ORIGINAL (sin toUpperCase) para preservar case del patrón
        val format = rawDataType.trim.replaceAll("(?i)DATE\\s*\\((.+)\\)", "$1").trim
        Some((colName, sum(when(notEmpty && to_date(c, format).isNull, 1).otherwise(0)), "DATE"))
      case _ => None
    }
  }

  // Valida que las columnas con nullable=false no tengan valores nulos o vacíos
  def validateNulls(df: DataFrame, reglas: Array[Row]): List[ValidationError] = {
    val notNullCols = reglas.filter(r => !Option(r.getAs[Boolean]("nullable")).getOrElse(true))
      .map(_.getAs[String]("field_name"))

    if (notNullCols.isEmpty) return List.empty

    val checks = notNullCols.map(c => sum(when(col(c).isNull || trim(col(c)) === "", 1).otherwise(0)))
    val result = df.select(checks: _*).first()

    notNullCols.zipWithIndex.collect {
      case (colName, idx) if result.getLong(idx) > 0 =>
        ValidationError(
          columnName = colName,
          errorMessage = s"[NULOS] Columna '$colName': ${result.getLong(idx)} valores nulos/vacíos (nullable=false)",
          errorType = Some("NOT_NULL_ERROR")
        )
    }.toList
  }

  // Valida que la longitud de los valores no exceda el máximo definido en length de semantic_layer
  def validateLengths(df: DataFrame, reglas: Array[Row]): List[ValidationError] = {
    val colsWithLen = reglas.flatMap { r =>
      Option(r.getAs[String]("length")).filter(_.nonEmpty).map(l => (r.getAs[String]("field_name"), l.toInt))
    }

    if (colsWithLen.isEmpty) return List.empty

    val checks = colsWithLen.map { case (c, len) => sum(when(col(c).isNotNull && length(col(c)) > len, 1).otherwise(0)) }
    val result = df.select(checks: _*).first()

    colsWithLen.zipWithIndex.collect {
      case ((colName, maxLen), idx) if result.getLong(idx) > 0 =>
        ValidationError(
          columnName = colName,
          errorMessage = s"[LONGITUD] Columna '$colName': ${result.getLong(idx)} valores exceden longitud máxima de $maxLen",
          errorType = Some("LENGTH_ERROR")
        )
    }.toList
  }

  // Valida que la combinación de columnas PK sea única en todo el DF
  def validatePrimaryKey(df: DataFrame, pkColumns: Seq[String]): List[ValidationError] = {
    if (pkColumns.isEmpty) return List.empty

    val pkCols = pkColumns.toList
    val duplicates = df.groupBy(pkCols.map(col): _*)
      .count()
      .filter(col("count") > 1)
      .agg(sum(col("count") - 1))
      .first().get(0)

    val count = if (duplicates == null) 0L else duplicates.asInstanceOf[Long]
    if (count > 0) {
      List(ValidationError(
        columnName = pkCols.mkString(","),
        errorMessage = s"[PK] Clave primaria (${pkCols.mkString(", ")}): $count registros duplicados",
        errorType = Some("PK_ERROR")
      ))
    } else List.empty
  }
}