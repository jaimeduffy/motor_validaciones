package org.yiyit

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties
import java.sql.DriverManager
import org.yiyit.utils.DbLogger
import org.yiyit.validations.{TableValidator, TechnicalValidator}

object App {
  def main(args: Array[String]): Unit = {

    // --- CONFIGURACIÓN INICIAL ---
    val spark = SparkSession.builder()
      .appName("ValidacionBigData")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Tomar los parámetros de configuración del application.properties
    val props = new Properties()
    val propertiesFile = getClass.getResourceAsStream("/application.properties")
    if (propertiesFile != null) props.load(propertiesFile) else System.exit(1)

    val jdbcUrl = props.getProperty("jdbc.url")
    val connectionProps = new Properties()
    connectionProps.put("user", props.getProperty("jdbc.user"))
    connectionProps.put("password", props.getProperty("jdbc.password"))
    connectionProps.put("driver", props.getProperty("jdbc.driver"))

    println("\n--> Iniciando motor de validación ...")

    try {
      // Conectamos con la base de datos y buscamos tareas pendientes en trigger_control (Flag = 0)
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
          // Actualizar flag a 11
          updateTriggerFlag(idTrigger, 11, jdbcUrl, props)
          println("--> Flag actualizado a 11 (Procesando...)")

          // Obtenemos la informacion de TABLE_CONFIGURATION
          val queryConfig = s"(SELECT * FROM public.table_configuration WHERE id_type_table = '$idTypeTable') as config"
          val configDf = spark.read.jdbc(jdbcUrl, queryConfig, connectionProps)

          if (configDf.isEmpty) {
            throw new Exception(s"No se encontró configuración en table_configuration para id_type_table: $idTypeTable")
          }

          val configRow = configDf.first()
          val hasHeader = configRow.getAs[Boolean]("header")
          println(s"--> Configuración cargada: header = $hasHeader")

          // Cargamos los datos desde la tabla madre
          val dataDf = spark.read.jdbc(jdbcUrl, tableName, connectionProps)
          println("--> Datos cargados...")

          // Obtener columnas esperadas ordenadas por field_position
          val queryCols = s"""(
            SELECT field_name
            FROM public.semantic_layer
            WHERE id_type_table = '$idTypeTable'
            ORDER BY field_position
          ) as cols"""
          val expectedColsDf = spark.read.jdbc(jdbcUrl, queryCols, connectionProps)
          val expectedColumns = expectedColsDf.collect().map(_.getAs[String]("field_name")).toSeq

          // =======================
          // FASE 1: TABLE VALIDATOR
          // =======================
          println("\n--> [FASE 1] Ejecutando TableValidator (Validación de Fichero)...")

          val resultadoEstructura = TableValidator.validateStructure(dataDf, expectedColumns, hasHeader)

          if (!resultadoEstructura.success) {
            val errorMsg = resultadoEstructura.errorMessage.getOrElse("Error desconocido")
            println(s"   FALLO: $errorMsg")
            DbLogger.logError(
              props,
              idTrigger,
              tableName,
              "ALL_COLUMNS",
              errorMsg,
              "technical_validation",
              "STRUCTURE_MISMATCH",
              "1"
            )
            updateTriggerFlag(idTrigger, 31, jdbcUrl, props)
            println("   Flag actualizado a 31 (Technical validation error)")

          } else {
            if (hasHeader) {
              println("   Estructura correcta. Columnas validadas.")
            } else {
              println("   Estructura correcta. Columnas renombradas.")
            }

            val validatedDf = resultadoEstructura.dataFrame.get

            // ============================
            // FASE 2: TECHNICAL VALIDATOR
            // ============================
            println("\n--> [FASE 2] Ejecutando TechnicalValidator...")

            val queryRules = s"(SELECT * FROM public.semantic_layer WHERE id_type_table = '$idTypeTable') as rules"
            val rulesDf = spark.read.jdbc(jdbcUrl, queryRules, connectionProps)
            val reglas = rulesDf.collect()

            val pkColumns = reglas.filter(r => Option(r.getAs[Boolean]("pk")).getOrElse(false))
              .map(_.getAs[String]("field_name"))
              .toSeq

            val techResult = TechnicalValidator.validate(validatedDf, reglas, pkColumns)

            if (!techResult.success) {
              techResult.errors.foreach { msg =>
                println(s"      $msg")

                val codigoError = techResult.phase match {
                  case "TIPO_DATOS" => "DATA_TYPE_VIOLATION"
                  case "NULOS" => "NOT_NULL_VIOLATION"
                  case "LONGITUD" => "LENGTH_VIOLATION"
                  case "PRIMARY_KEY" => "PK_VIOLATION"
                  case _ => "VALIDATION_ERROR"
                }

                val colName = extractColumnName(msg)

                DbLogger.logError(
                  props,
                  idTrigger,
                  tableName,
                  colName,
                  msg,
                  "technical_validation",
                  codigoError,
                  "1"
                )
              }

              println(s"\n   Se encontraron ${techResult.errors.size} errores en fase ${techResult.phase}.")
              updateTriggerFlag(idTrigger, 32, jdbcUrl, props)
              println("   Flag actualizado a 32 (Technical validation error)")

            } else {
              println("   Validaciones técnicas superadas.")

              // ===================================
              // FASE 3: INTEGRIDAD REFERENCIAL
              // ===================================
              // TODO: Implementar ReferentialIntegrityValidator
              println("\n--> [FASE 3] ReferentialIntegrityValidator - TODO")

              // ===================================
              // FASE 4: VALIDACIONES FUNCIONALES
              // ===================================
              // TODO: Implementar FunctionalValidator
              println("\n--> [FASE 4] FunctionalValidator - TODO")

              // ÉXITO TOTAL (FLAG 2)
              updateTriggerFlag(idTrigger, 2, jdbcUrl, props)
              println("\n   VALIDACIÓN COMPLETADA")
              println("   Flag actualizado a 2 (Validation OK)")
            }
          }

        } catch {
          case e: Exception =>
            println(s"\n   ERROR DE SISTEMA: ${e.getMessage}")
            e.printStackTrace()
            updateTriggerFlag(idTrigger, 3, jdbcUrl, props)
            DbLogger.logError(
              props,
              idTrigger,
              tableName,
              "SYSTEM",
              e.getMessage,
              "system_error",
              "EXECUTION_ERROR",
              "1"
            )
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

  private def extractColumnName(msg: String): String = {
    val pattern = """Columna '([^']+)'""".r
    pattern.findFirstMatchIn(msg) match {
      case Some(m) => m.group(1)
      case None =>
        val pkPattern = """Clave primaria \(([^)]+)\)""".r
        pkPattern.findFirstMatchIn(msg) match {
          case Some(m) => m.group(1)
          case None => "UNKNOWN"
        }
    }
  }

  def updateTriggerFlag(id: Int, newFlag: Int, url: String, props: Properties): Unit = {
    val conn = DriverManager.getConnection(
      url,
      props.getProperty("jdbc.user"),
      props.getProperty("jdbc.password")
    )
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