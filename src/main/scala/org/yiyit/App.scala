package org.yiyit

import org.apache.spark.sql.SparkSession
import java.util.Properties
import java.sql.DriverManager
import org.yiyit.utils.DbLogger
import org.yiyit.validations.{TableValidator, TechnicalValidator}

object App {
  def main(args: Array[String]): Unit = {

    // Creamos el SparkSession
    val spark = SparkSession.builder()
      .appName("ValidacionBigData")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Conexión con la BBDD
    val props = new Properties()
    val propertiesFile = getClass.getResourceAsStream("/application.properties")
    if (propertiesFile != null) props.load(propertiesFile) else System.exit(1)

    val jdbcUrl = props.getProperty("jdbc.url")
    val connectionProps = new Properties()
    connectionProps.put("user", props.getProperty("jdbc.user"))
    connectionProps.put("password", props.getProperty("jdbc.password"))
    connectionProps.put("driver", props.getProperty("jdbc.driver"))

    println("\n--> INICIANDO MOTOR DE VALIDACIÓN ...")

    try {
      // Tomamos los valores de trigger_control
      // Nos quedamos con los triggers en estado pendientes ("flag = 0")
      val triggerDf = spark.read.jdbc(jdbcUrl, "public.trigger_control", connectionProps)
      val pendientes = triggerDf.filter("flag = 0").collect()

      if (pendientes.isEmpty) println("--> No hay tareas pendientes.")

      pendientes.foreach { row =>
        val idTrigger = row.getAs[Int]("id_trigger")
        val tableName = row.getAs[String]("table_name")
        val idTypeTable = row.getAs[String]("id_type_table")

        println(s"\n========================================================")
        println(s"  PROCESANDO TRIGGER ID: $idTrigger")
        println(s"  Tabla: $tableName | id_type_table: $idTypeTable")
        println(s"========================================================")

        try {
          updateTriggerFlag(idTrigger, 11, jdbcUrl, props)
          println("--> Flag actualizado a 11 (Procesando...)")

          // Cargar configuración
          val configDf = spark.read.jdbc(jdbcUrl, s"(SELECT * FROM public.table_configuration WHERE id_type_table = '$idTypeTable') as t", connectionProps)
          val hasHeader = configDf.first().getAs[Boolean]("header")
          println(s"--> Configuración cargada: header = $hasHeader")

          // Cargar datos
          val dataDf = spark.read.jdbc(jdbcUrl, tableName, connectionProps)
          println(s"--> Datos cargados: ${dataDf.columns.length} columnas")

          // Cargar reglas
          val rulesDf = spark.read.jdbc(jdbcUrl, s"(SELECT * FROM public.semantic_layer WHERE id_type_table = '$idTypeTable' ORDER BY field_position) as r", connectionProps)
          val reglas = rulesDf.collect()
          val expectedColumns = reglas.map(_.getAs[String]("field_name")).toSeq

          // =======================
          // FASE 1: ESTRUCTURA
          // =======================
          println("\n--> [FASE 1] Validando estructura...")
          val estructuraResult = TableValidator.validateStructure(dataDf, expectedColumns, hasHeader)

          if (!estructuraResult.success) {
            // Caso ERROR
            val errorMsg = estructuraResult.errorMessage.getOrElse("Error desconocido")
            println(s"   FALLO: $errorMsg")
            DbLogger.logError(props, idTrigger, tableName, "ALL_COLUMNS", errorMsg, "technical_validation", "STRUCTURE_MISMATCH", "1")
            updateTriggerFlag(idTrigger, 31, jdbcUrl, props)
            println("   Flag actualizado a 31")
          } else {
            // Caso ÉXITO
            println(s"   OK")
            updateTriggerFlag(idTrigger, 12, jdbcUrl, props)
            println("--> Flag actualizado a 12 (Validaciones de Tabla pasadas).")
            // Tomamos el DF validado
            val validatedDf = estructuraResult.dataFrame.get
            // Aplicamos filtro y map para tomar los nombres de las columnas que son pk
            val pkColumns = reglas.filter(r => Option(r.getAs[Boolean]("pk")).getOrElse(false)).map(_.getAs[String]("field_name")).toSeq

            // =======================
            // FASE 2: VALIDACIONES TÉCNICAS
            // =======================
            var fase2Errors: List[String] = List.empty
            var faseActual = ""

            // 2.1 Tipos de datos
            println("\n--> [FASE 2.1] Validando tipos de datos...")
            fase2Errors = TechnicalValidator.validateDataTypes(validatedDf, reglas)
            faseActual = "TIPO_DATOS"

            // 2.2 Nulos
            if (fase2Errors.isEmpty) {
              println("   OK")
              println("\n--> [FASE 2.2] Validando nulos...")
              fase2Errors = TechnicalValidator.validateNulls(validatedDf, reglas)
              faseActual = "NULOS"
            }

            // 2.3 Longitud
            if (fase2Errors.isEmpty) {
              println("   OK")
              println("\n--> [FASE 2.3] Validando longitud...")
              fase2Errors = TechnicalValidator.validateLengths(validatedDf, reglas)
              faseActual = "LONGITUD"
            }

            // 2.4 Primary Key (Solo si hay PK)
            if (fase2Errors.isEmpty && pkColumns.nonEmpty) {
              println("   OK")
              println(s"\n--> [FASE 2.4] Validando PK (${pkColumns.mkString(", ")})...")
              fase2Errors = TechnicalValidator.validatePrimaryKey(validatedDf, pkColumns)
              faseActual = "PRIMARY_KEY"
            }

            if (fase2Errors.nonEmpty) {
              println(s"   FALLO en $faseActual:")
              fase2Errors.foreach { msg =>
                println(s"      $msg")
                val codigoError = faseActual match {
                  case "TIPO_DATOS" => "DATA_TYPE_ERROR"
                  case "NULOS" => "NOT_NULL_ERROR"
                  case "LONGITUD" => "LENGTH_ERROR"
                  case "PRIMARY_KEY" => "PK_ERROR"
                }
                val colName = if (msg.contains("Columna '")) msg.split("'")(1)
                  else if (msg.contains("Clave primaria")) pkColumns.mkString(",")
                  else "UNKNOWN"
                DbLogger.logError(props, idTrigger, tableName, colName, msg, "technical_validation", codigoError, "1")
              }
              updateTriggerFlag(idTrigger, 32, jdbcUrl, props)
              println(s"\n   Flag actualizado a 32")
            } else {
              println("   OK")
              updateTriggerFlag(idTrigger, 13, jdbcUrl, props)
              println("--> Flag actualizado a 13 (Validaciones técnicas pasadas).")

              // =======================
              // FASE 3: INTEGRIDAD REFERENCIAL
              // =======================
              println("\n--> [FASE 3] Integridad Referencial - TODO")

              // =======================
              // FASE 4: VALIDACIONES FUNCIONALES
              // =======================
              println("\n--> [FASE 4] Validaciones Funcionales - TODO")

              updateTriggerFlag(idTrigger, 2, jdbcUrl, props)
              println("\n   VALIDACIÓN COMPLETADA - Flag actualizado a 2")
            }
          }

        } catch {
          case e: Exception =>
            println(s"\n   ERROR DE SISTEMA: ${e.getMessage}")
            updateTriggerFlag(idTrigger, 3, jdbcUrl, props)
            DbLogger.logError(props, idTrigger, tableName, "SYSTEM", e.getMessage, "system_error", "EXECUTION_ERROR", "1")
        }
      }

    } catch {
      case e: Exception =>
        println(s"Error fatal: ${e.getMessage}")
        e.printStackTrace()
    }

    spark.stop()
    println("\n--> Motor de validación finalizado.")
  }

  // Función privada para actualizar el Flag de la tabla trigger_control
  private def updateTriggerFlag(id: Int, newFlag: Int, url: String, props: Properties): Unit = {
    val conn = DriverManager.getConnection(url, props.getProperty("jdbc.user"), props.getProperty("jdbc.password"))
    try {
      val stmt = conn.prepareStatement("UPDATE public.trigger_control SET flag = ? WHERE id_trigger = ?")
      stmt.setInt(1, newFlag)
      stmt.setInt(2, id)
      stmt.executeUpdate()
      stmt.close()
    } finally {
      conn.close()
    }
  }
}