package org.yiyit.validations

import org.apache.spark.sql.DataFrame

object TableValidator {
  /**
   * Valida la estructura general de la tabla.
   *
   * @param dataDf          El DataFrame con los datos físicos reales.
   * @param expectedColumns Lista de nombres de columnas que OBLIGATORIAMENTE deben existir (desde semantic_layer).
   * @return                Some("Mensaje de Error") si falla, o None si esta correcto.
   */
  def validateStructure(dataDf: DataFrame, expectedColumns: Seq[String]): Option[String] = {

    // Pasamos a minúsculas para evitar errores tipográficos
    val columnasReales = dataDf.columns.map(_.toLowerCase)
    val columnasEsperadas = expectedColumns.map(_.toLowerCase)

    // Comprobación de integridad de columnas
    val columnasFaltantes = columnasEsperadas.filterNot(col => columnasReales.contains(col))

    if (columnasFaltantes.nonEmpty) {
      // CASO DE ERROR
      val msg = s"Error de Estructura: Faltan ${columnasFaltantes.length} columnas obligatorias: [${columnasFaltantes.mkString(", ")}]"
      Some(msg)
    } else {
      if (dataDf.isEmpty) {
        Some("Error de Estructura: La tabla está completamente vacía (0 filas).")
      } else {
        // ÉXITO
        None
      }
    }
  }
}