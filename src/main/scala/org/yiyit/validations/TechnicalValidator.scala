package org.yiyit.validations

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

/**
 * Validador Técnico (Fase 2)
 *
 * Flujo secuencial:
 * 2.1 Tipo de datos -> si falla, PARA
 * 2.2 Nulos -> si falla, PARA
 * 2.3 Longitud -> si falla, PARA
 * 2.4 Primary Key -> si falla, PARA
 */
object TechnicalValidator {

  case class ValidationResult(
                               success: Boolean,
                               errors: List[String],
                               phase: String
                             )

  /**
   * Ejecuta todas las validaciones técnicas en orden.
   */
  def validate(dataDf: DataFrame, reglas: Array[Row], pkColumns: Seq[String]): ValidationResult = {

    // Cachear el DataFrame para evitar múltiples lecturas
    dataDf.cache()

    try {
      // FASE 2.1: Tipo de datos
      println("   [2.1] Validando tipos de datos...")
      val typeErrors = validateAllDataTypes(dataDf, reglas)
      if (typeErrors.nonEmpty) {
        println("   [2.1] Tipo de datos: FALLO")
        return ValidationResult(success = false, errors = typeErrors, phase = "TIPO_DATOS")
      }
      println("   [2.1] Tipo de datos: OK")

      // FASE 2.2: Nulos
      println("   [2.2] Validando nulos...")
      val nullErrors = validateAllNulls(dataDf, reglas)
      if (nullErrors.nonEmpty) {
        println("   [2.2] Nulos: FALLO")
        return ValidationResult(success = false, errors = nullErrors, phase = "NULOS")
      }
      println("   [2.2] Nulos: OK")

      // FASE 2.3: Longitud
      println("   [2.3] Validando longitud...")
      val lengthErrors = validateAllLengths(dataDf, reglas)
      if (lengthErrors.nonEmpty) {
        println("   [2.3] Longitud: FALLO")
        return ValidationResult(success = false, errors = lengthErrors, phase = "LONGITUD")
      }
      println("   [2.3] Longitud: OK")

      // FASE 2.4: Primary Key
      if (pkColumns.nonEmpty) {
        println(s"   [2.4] Validando PK (${pkColumns.mkString(", ")})...")
        val pkErrors = validatePrimaryKey(dataDf, pkColumns)
        if (pkErrors.nonEmpty) {
          println("   [2.4] Primary Key: FALLO")
          return ValidationResult(success = false, errors = pkErrors, phase = "PRIMARY_KEY")
        }
        println("   [2.4] Primary Key: OK")
      }

      ValidationResult(success = true, errors = List.empty, phase = "COMPLETE")

    } finally {
      dataDf.unpersist()
    }
  }

  // ========================
  // FASE 2.1: TIPO DE DATOS
  // ========================
  private def validateAllDataTypes(dataDf: DataFrame, reglas: Array[Row]): List[String] = {
    val errores = scala.collection.mutable.ListBuffer[String]()

    // Agrupar columnas por tipo para validar en menos pasadas
    val intColumns = reglas.filter(r =>
      Option(r.getAs[String]("data_type")).getOrElse("").toUpperCase == "INT"
    ).map(_.getAs[String]("field_name"))

    val decimalColumns = reglas.filter(r =>
      Option(r.getAs[String]("data_type")).getOrElse("").toUpperCase.startsWith("DECIMAL")
    )

    val dateColumns = reglas.filter(r =>
      Option(r.getAs[String]("data_type")).getOrElse("").toUpperCase.startsWith("DATE")
    )

    // Validar INT en una sola pasada
    if (intColumns.nonEmpty) {
      errores ++= validateIntColumns(dataDf, intColumns)
    }

    // Validar DECIMAL
    decimalColumns.foreach { regla =>
      val colName = regla.getAs[String]("field_name")
      val dataType = regla.getAs[String]("data_type").toUpperCase
      val decimalSymbol = Option(regla.getAs[String]("decimal_symbol")).getOrElse(".")
      errores ++= validateDecimalType(dataDf, colName, dataType, decimalSymbol)
    }

    // Validar DATE
    dateColumns.foreach { regla =>
      val colName = regla.getAs[String]("field_name")
      val dataType = regla.getAs[String]("data_type").toUpperCase
      errores ++= validateDateType(dataDf, colName, dataType)
    }

    errores.toList
  }

  private def validateIntColumns(dataDf: DataFrame, columns: Array[String]): List[String] = {
    val errores = scala.collection.mutable.ListBuffer[String]()

    // Construir expresiones para todas las columnas INT en una sola query
    val conditions = columns.map { colName =>
      sum(when(
        col(colName).isNotNull &&
          trim(col(colName)) =!= "" &&
          !col(colName).rlike("^-?[0-9]+$"), 1
      ).otherwise(0)).alias(colName)
    }

    val counts = dataDf.select(conditions: _*).first()

    columns.zipWithIndex.foreach { case (colName, idx) =>
      val invalidCount = counts.getLong(idx)
      if (invalidCount > 0) {
        errores += s"[TIPO] Columna '$colName': $invalidCount valores no son INT válidos"
      }
    }

    errores.toList
  }

  private def validateDecimalType(dataDf: DataFrame, colName: String, decimalType: String, decimalSymbol: String): List[String] = {
    val pattern = """DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)""".r

    decimalType match {
      case pattern(precisionStr, scaleStr) =>
        val precision = precisionStr.toInt
        val scale = scaleStr.toInt
        val escapedSymbol = if (decimalSymbol == ".") "\\." else decimalSymbol
        val decimalRegex = s"^-?[0-9]+($escapedSymbol[0-9]+)?$$"

        val invalidCount = dataDf.filter(
          col(colName).isNotNull &&
            trim(col(colName)) =!= "" &&
            !col(colName).rlike(decimalRegex)
        ).count()

        if (invalidCount > 0) {
          List(s"[TIPO] Columna '$colName': $invalidCount valores no son DECIMAL válidos")
        } else {
          List.empty
        }

      case _ =>
        List(s"[TIPO] Columna '$colName': Formato '$decimalType' no reconocido")
    }
  }

  private def validateDateType(dataDf: DataFrame, colName: String, dateType: String): List[String] = {
    val pattern = """DATE\s*\(\s*(.+)\s*\)""".r

    dateType match {
      case pattern(dateFormat) =>
        val trimmedFormat = dateFormat.trim

        val invalidCount = dataDf.filter(
          col(colName).isNotNull &&
            trim(col(colName)) =!= "" &&
            to_date(col(colName), trimmedFormat).isNull
        ).count()

        if (invalidCount > 0) {
          List(s"[TIPO] Columna '$colName': $invalidCount valores no cumplen formato DATE($trimmedFormat)")
        } else List.empty

      case _ =>
        List(s"[TIPO] Columna '$colName': Formato '$dateType' no reconocido")
    }
  }

  // ===========================================
  // FASE 2.2: NULOS (validación en una sola pasada)
  // ===========================================
  private def validateAllNulls(dataDf: DataFrame, reglas: Array[Row]): List[String] = {
    // Filtrar solo columnas con nullable=false
    val notNullableColumns = reglas.filter { r =>
      !Option(r.getAs[Boolean]("nullable")).getOrElse(true)
    }.map(_.getAs[String]("field_name"))

    if (notNullableColumns.isEmpty) {
      return List.empty
    }

    // Construir expresiones para contar nulos de todas las columnas en una sola query
    val conditions = notNullableColumns.map { colName =>
      sum(when(col(colName).isNull || trim(col(colName)) === "", 1).otherwise(0)).alias(colName)
    }

    val counts = dataDf.select(conditions: _*).first()

    val errores = scala.collection.mutable.ListBuffer[String]()
    notNullableColumns.zipWithIndex.foreach { case (colName, idx) =>
      val nullCount = counts.getLong(idx)
      if (nullCount > 0) {
        errores += s"[NULOS] Columna '$colName': $nullCount valores nulos/vacíos (nullable=false)"
      }
    }

    errores.toList
  }

  // ===========================================
  // FASE 2.3: LONGITUD (validación en una sola pasada)
  // ===========================================
  private def validateAllLengths(dataDf: DataFrame, reglas: Array[Row]): List[String] = {
    // Filtrar columnas con longitud definida
    val columnsWithLength = reglas.filter { r =>
      Option(r.getAs[String]("length")).exists(_.nonEmpty)
    }.map { r =>
      (r.getAs[String]("field_name"), r.getAs[String]("length").toInt)
    }

    if (columnsWithLength.isEmpty) {
      return List.empty
    }

    // Construir expresiones para todas las columnas en una sola query
    val conditions = columnsWithLength.map { case (colName, maxLen) =>
      sum(when(col(colName).isNotNull && length(col(colName)) > maxLen, 1).otherwise(0)).alias(colName)
    }

    val counts = dataDf.select(conditions: _*).first()

    val errores = scala.collection.mutable.ListBuffer[String]()
    columnsWithLength.zipWithIndex.foreach { case ((colName, maxLen), idx) =>
      val invalidCount = counts.getLong(idx)
      if (invalidCount > 0) {
        errores += s"[LONGITUD] Columna '$colName': $invalidCount valores exceden longitud máxima de $maxLen"
      }
    }

    errores.toList
  }

  // ===========================================
  // FASE 2.4: PRIMARY KEY
  // ===========================================
  def validatePrimaryKey(dataDf: DataFrame, pkColumns: Seq[String]): List[String] = {
    if (pkColumns.isEmpty) {
      return List.empty
    }

    // Usar groupBy + count para encontrar duplicados más eficientemente
    val duplicateCount = dataDf
      .groupBy(pkColumns.map(col): _*)
      .count()
      .filter(col("count") > 1)
      .agg(sum(col("count") - 1))
      .first()
      .get(0)

    val duplicates = if (duplicateCount == null) 0L else duplicateCount.asInstanceOf[Long]

    if (duplicates > 0) {
      List(s"[PK] Clave primaria (${pkColumns.mkString(", ")}): $duplicates registros duplicados")
    } else List.empty
  }
}