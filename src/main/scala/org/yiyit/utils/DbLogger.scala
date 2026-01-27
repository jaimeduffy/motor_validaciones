package org.yiyit.utils

import java.sql.DriverManager
import java.util.Properties

object DbLogger {

  /**
   * Registra un error en la base de datos ajustándose a la definición real de la tabla.
   * * @param validationId  Código del error (ej: "STRUCTURE_ERROR", "NOT_NULL_ERROR"). OBLIGATORIO.
   * @param incidences    Número de fallos encontrados (como String). OBLIGATORIO.
   */
  def logError(props: Properties,
               idTrigger: Int,
               tableName: String,
               fieldName: String,
               msg: String,
               validationType: String,
               validationId: String,
               incidences: String
              ): Unit = {
    try {
      val jdbcUrl = props.getProperty("jdbc.url")
      val conn = DriverManager.getConnection(
        jdbcUrl,
        props.getProperty("jdbc.user"),
        props.getProperty("jdbc.password")
      )

      // Recortamos el mensaje por seguridad
      val mensajeSeguro = if (msg.length > 255) msg.take(250) + "..." else msg

      // SQL ajustado
      val sql = """
        INSERT INTO public.process_validation_logs
        (id_trigger, validation_id, type_validation, table_name, validation_msg, field_name, incidences, flag, execution_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1, NOW())
      """

      val prepStmt = conn.prepareStatement(sql)
      prepStmt.setInt(1, idTrigger)
      prepStmt.setString(2, validationId)
      prepStmt.setString(3, validationType)
      prepStmt.setString(4, tableName)
      prepStmt.setString(5, mensajeSeguro)
      prepStmt.setString(6, fieldName)
      prepStmt.setString(7, incidences)

      prepStmt.executeUpdate()
      conn.close()

    } catch {
      case e: Exception =>
        println(s"   [ERROR INTERNO] No se pudo escribir el log en BBDD: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}