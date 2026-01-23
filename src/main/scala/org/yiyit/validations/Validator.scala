package org.yiyit.validations

import org.apache.spark.sql.{DataFrame, Row}

// Este trait es la interfaz que siguen todas nuestras validaciones
trait Validator {
  /**
   * @param dataDf    El DataFrame con los datos reales
   * @param rule      La fila de configuración (semantic_layer) con las reglas
   * @param columnName El nombre de la columna a validar
   * @return          Some("Mensaje de error") si falla, o None si está
   */
  def validate(dataDf: DataFrame, rule: Row, columnName: String): Option[String]
}
