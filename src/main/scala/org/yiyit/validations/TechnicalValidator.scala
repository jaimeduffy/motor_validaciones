package org.yiyit.validations

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, length}

object TechnicalValidator {

  /**
   * Ejecuta todas las validaciones técnicas sobre UNA columna.
   * Corresponde al paso de Flag 1.2 -> 1.3
   *
   * @param dataDf      El DataFrame con los datos.
   * @param rule        La fila de 'semantic_layer' con la configuración (nullable, length, type).
   * @param columnName  El nombre de la columna que estamos validando.
   * @return            Una lista (Seq) de mensajes de error. Si está vacía, es que todo está OK.
   */
  def validateColumnRules(dataDf: DataFrame, rule: Row, columnName: String): Seq[String] = {
    var errores = Seq[String]()

    // --- VALIDACIÓN DE NULOS ---
    val permiteNulos = try { rule.getAs[Boolean]("nullable") } catch { case _: Exception => rule.getAs[String]("nullable").toBoolean }

    if (!permiteNulos) {
      // Contamos nulos reales
      val nulos = dataDf.filter(col(columnName).isNull).count()
      if (nulos > 0) {
        errores = errores :+ s"[NULOS] La columna '$columnName' no admite nulos pero tiene $nulos registros vacíos."
      }
    }

    // --- VALIDACIÓN DE LONGITUD ---
    if (!rule.isNullAt(rule.fieldIndex("length"))) {
      val maxLongitud = try { rule.getAs[Int]("length") } catch { case _: Exception => rule.getAs[String]("length").toInt }

      val muyLargos = dataDf.filter(length(col(columnName)) > maxLongitud).count()

      if (muyLargos > 0) {
        errores = errores :+ s"[LONGITUD] La columna '$columnName' excede el máximo de $maxLongitud caracteres en $muyLargos registros."
      }
    }

    errores
  }
}