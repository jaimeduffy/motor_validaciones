package org.yiyit.validations

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

object TechnicalValidator {

  // Valida tipos de datos de todas las columnas
  def validateDataTypes(df: DataFrame, reglas: Array[Row]): List[String] = {
    val checks = reglas.flatMap { r =>
      val colName = r.getAs[String]("field_name")
      val dataType = Option(r.getAs[String]("data_type")).getOrElse("STRING").toUpperCase
      buildTypeCheck(colName, dataType)
    }

    if (checks.isEmpty) return List.empty

    val result = df.select(checks.map(_._2): _*).first()
    checks.zipWithIndex.collect {
      case ((colName, _, dataType), idx) if result.getLong(idx) > 0 =>
        s"[TIPO] Columna '$colName': ${result.getLong(idx)} valores no son $dataType válidos"
    }.toList
  }

  private def buildTypeCheck(colName: String, dataType: String): Option[(String, org.apache.spark.sql.Column, String)] = {
    val c = col(colName)
    val notEmpty = c.isNotNull && trim(c) =!= ""

    dataType match {
      case "STRING" => None
      case "INT" =>
        Some((colName, sum(when(notEmpty && !c.rlike("^-?[0-9]+$"), 1).otherwise(0)), "INT"))
      case d if d.startsWith("DECIMAL") =>
        Some((colName, sum(when(notEmpty && !c.rlike("^-?[0-9]+(\\.[0-9]+)?$"), 1).otherwise(0)), "DECIMAL"))
      case d if d.startsWith("DATE") =>
        val format = d.replaceAll("DATE\\s*\\((.+)\\)", "$1").trim
        Some((colName, sum(when(notEmpty && to_date(c, format).isNull, 1).otherwise(0)), "DATE"))
      case _ => None
    }
  }

  // Valida nulos en columnas con nullable=false
  def validateNulls(df: DataFrame, reglas: Array[Row]): List[String] = {
    val notNullCols = reglas.filter(r => !Option(r.getAs[Boolean]("nullable")).getOrElse(true))
      .map(_.getAs[String]("field_name"))

    if (notNullCols.isEmpty) return List.empty

    val checks = notNullCols.map(c => sum(when(col(c).isNull || trim(col(c)) === "", 1).otherwise(0)))
    val result = df.select(checks: _*).first()

    notNullCols.zipWithIndex.collect {
      case (colName, idx) if result.getLong(idx) > 0 =>
        s"[NULOS] Columna '$colName': ${result.getLong(idx)} valores nulos/vacíos (nullable=false)"
    }.toList
  }

  // Valida longitud máxima de columnas
  def validateLengths(df: DataFrame, reglas: Array[Row]): List[String] = {
    val colsWithLen = reglas.flatMap { r =>
      Option(r.getAs[String]("length")).filter(_.nonEmpty).map(l => (r.getAs[String]("field_name"), l.toInt))
    }

    if (colsWithLen.isEmpty) return List.empty

    val checks = colsWithLen.map { case (c, len) => sum(when(col(c).isNotNull && length(col(c)) > len, 1).otherwise(0)) }
    val result = df.select(checks: _*).first()

    colsWithLen.zipWithIndex.collect {
      case ((colName, maxLen), idx) if result.getLong(idx) > 0 =>
        s"[LONGITUD] Columna '$colName': ${result.getLong(idx)} valores exceden longitud máxima de $maxLen"
    }.toList
  }

  // Valida unicidad de Primary Key compuesta
  def validatePrimaryKey(df: DataFrame, pkColumns: Seq[String]): List[String] = {
    if (pkColumns.isEmpty) return List.empty

    val duplicates = df.groupBy(pkColumns.map(col): _*)
      .count()
      .filter(col("count") > 1)
      .agg(sum(col("count") - 1))
      .first().get(0)

    val count = if (duplicates == null) 0L else duplicates.asInstanceOf[Long]
    if (count > 0) List(s"[PK] Clave primaria (${pkColumns.mkString(", ")}): $count registros duplicados")
    else List.empty
  }
}