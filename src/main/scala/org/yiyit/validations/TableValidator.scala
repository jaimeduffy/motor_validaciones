package org.yiyit.validations

import org.apache.spark.sql.DataFrame

/**
 * Validador de estructura de tablas
 *
 * Según el enunciado:
 * - Si header = true: Los nombres de columnas deben coincidir exactamente con field_name
 * - Si header = false: La validación pasa, pero hay que renombrar las columnas
 */
object TableValidator {

  /**
   * Case class para definir la estructura del resultado de la validación:
   * success: true si ha pasado la validación, false en caso contrario
   * errorMessage: Mensaje en caso de error
   * dataFrame: DF final validado, con las columnas renombradas si hiciera falta
   */
  case class ValidationResult(
                               success: Boolean,
                               errorMessage: Option[String],
                               dataFrame: Option[DataFrame]
                             )

  /**
   * Valida la estructura de la tabla según el valor de header:
   * @param dataDf          DataFrame con los datos de reales
   * @param expectedColumns Lista de nombres de columnas esperadas
   * @param hasHeader       Valor del campo 'header' de file_configuration
   * @return ValidationResult con el resultado de la validación
   */
  def validateStructure(dataDf: DataFrame, expectedColumns: Seq[String], hasHeader: Boolean): ValidationResult = {

    // Validación: tabla vacía
    if (dataDf.isEmpty) {
      return ValidationResult(
        success = false,
        errorMessage = Some("Error de Estructura: La tabla está completamente vacía (0 filas)."),
        dataFrame = None
      )
    }

    // Validación: número de columnas debe coincidir
    val numColumnasReales = dataDf.columns.length
    val numColumnasEsperadas = expectedColumns.length

    if (numColumnasReales != numColumnasEsperadas) {
      return ValidationResult(
        success = false,
        errorMessage = Some(s"Error de Estructura: El número de columnas no coincide. " +
          s"Esperadas: $numColumnasEsperadas, Encontradas: $numColumnasReales"),
        dataFrame = None
      )
    }

    if (hasHeader) {
      // CASO header = true: Los nombres DEBEN coincidir exactamente
      validateWithHeader(dataDf, expectedColumns)
    } else {
      // CASO header = false: No validar nombres, solo renombrar
      renameColumns(dataDf, expectedColumns)
    }
  }

  /**
   * Valida cuando header = true.
   * Los nombres de columnas del DataFrame deben coincidir exactamente con los esperados.
   */
  private def validateWithHeader(dataDf: DataFrame, expectedColumns: Seq[String]): ValidationResult = {
    val columnasReales = dataDf.columns.map(_.toLowerCase)
    val columnasEsperadas = expectedColumns.map(_.toLowerCase)

    // Verificar que cada columna esperada existe en las reales
    val columnasFaltantes = columnasEsperadas.filterNot(col => columnasReales.contains(col))

    // Verificar columnas extra que no deberían estar
    val columnasExtra = columnasReales.filterNot(col => columnasEsperadas.contains(col))

    if (columnasFaltantes.nonEmpty || columnasExtra.nonEmpty) {
      val errores = scala.collection.mutable.ListBuffer[String]()

      if (columnasFaltantes.nonEmpty) {
        errores += s"Columnas faltantes: [${columnasFaltantes.mkString(", ")}]"
      }
      if (columnasExtra.nonEmpty) {
        errores += s"Columnas no esperadas: [${columnasExtra.mkString(", ")}]"
      }

      ValidationResult(
        success = false,
        errorMessage = Some(s"Error de Estructura (header=true): ${errores.mkString(". ")}"),
        dataFrame = None
      )
    } else {
      ValidationResult(
        success = true,
        errorMessage = None,
        dataFrame = Some(dataDf)
      )
    }
  }

  /**
   * Renombra las columnas del DataFrame con los nombres esperados.
   * Asumo que el orden de las columnas corresponde con el orden de expectedColumns.
   */
  private def renameColumns(dataDf: DataFrame, expectedColumns: Seq[String]): ValidationResult = {
    try {
      val columnasActuales = dataDf.columns

      // Crear el DataFrame con columnas renombradas
      val renamedDf = columnasActuales.zip(expectedColumns).foldLeft(dataDf) {
        case (df, (oldName, newName)) =>
          df.withColumnRenamed(oldName, newName)
      }

      ValidationResult(
        success = true,
        errorMessage = None,
        dataFrame = Some(renamedDf)
      )
    } catch {
      case e: Exception =>
        ValidationResult(
          success = false,
          errorMessage = Some(s"Error al renombrar columnas: ${e.getMessage}"),
          dataFrame = None
        )
    }
  }
}