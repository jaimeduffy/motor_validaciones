package org.yiyit

import org.apache.spark.sql.SparkSession
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
      // --- BUSCAR TAREAS PENDIENTES (Flag = 0) ---
      val triggerDf = spark.read.jdbc(jdbcUrl, "public.trigger_control", connectionProps)
      val pendientes = triggerDf.filter("flag = 0").collect()

      if (pendientes.isEmpty) println("--> No hay tareas pendientes.")

      pendientes.foreach { row =>
        val idTrigger = row.getAs[Int]("id_trigger")
        val tableName = row.getAs[String]("table_name")
        val idTypeTable = row.getAs[String]("id_type_table")

        println(s"\n--- PROCESANDO TRIGGER ID: $idTrigger [Tabla: $tableName] ---")

        try {
          // Cargar datos físicos
          val dataDf = spark.read.jdbc(jdbcUrl, tableName, connectionProps)

          // ==============================================================================
          // FASE 1: TABLE VALIDATOR (Estructura) -> Corresponde a Flag 1.1
          // ==============================================================================
          println("--> [FASE 1] Ejecutando TableValidator (Estructura)...")

          // Recuperar nombres de columnas esperadas desde la BBDD
          val queryCols = s"(SELECT field_name FROM public.semantic_layer WHERE id_type_table = '$idTypeTable') as cols"
          val expectedColsDf = spark.read.jdbc(jdbcUrl, queryCols, connectionProps)
          val expectedColumns = expectedColsDf.collect().map(_.getAs[String]("field_name"))

          // Validar
          val errorEstructura = TableValidator.validateStructure(dataDf, expectedColumns)

          if (errorEstructura.isDefined) {
            // --- CASO ERROR (FLAG 31) ---
            println(s"   FALLO CRÍTICO: ${errorEstructura.get}")

            // Loguear error
            DbLogger.logError(
              props,
              idTrigger,
              tableName,
              "ALL_COLUMNS",
              errorEstructura.get,
              "technical_validation",
              "STRUCTURE_MISMATCH",
              "1"
            )

            // Actualizar Trigger a 31 (Error Estructura)
            updateTriggerFlag(idTrigger, 31, jdbcUrl, props)

            println("  Proceso detenido para esta tabla.")

          } else {
            // --- CASO ÉXITO (Pasa a Flag 1.2) ---
            println("   Estructura Correcta. Avanzando a Validaciones Técnicas...")
            updateTriggerFlag(idTrigger, 1, jdbcUrl, props) // Usamos 1.2 (o 1 simplificado) para indicar progreso

            // ==============================================================================
            // FASE 2: TECHNICAL VALIDATOR (Columnas) -> Corresponde a Flag 1.2
            // ==============================================================================
            println("--> [FASE 2] Ejecutando TechnicalValidator (Nulos, Tipos, Longitud)...")

            // Recuperar todas las reglas completas
            val queryRules = s"(SELECT * FROM public.semantic_layer WHERE id_type_table = '$idTypeTable') as rules"
            val rulesDf = spark.read.jdbc(jdbcUrl, queryRules, connectionProps)
            val reglas = rulesDf.collect()

            var erroresTecnicos = 0

            reglas.foreach { regla =>
              val colName = regla.getAs[String]("field_name")

              // Ejecutar validador técnico
              val fallos = TechnicalValidator.validateColumnRules(dataDf, regla, colName)

              fallos.foreach { msg =>
                erroresTecnicos += 1
                println(s"      $msg")

                val codigoError = if (msg.contains("[NULOS]")) "NOT_NULL_VIOLATION"
                else if (msg.contains("[LONGITUD]")) "LENGTH_VIOLATION"
                else "DATA_TYPE_VIOLATION"


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
            }

            if (erroresTecnicos > 0) {
              // --- CASO ERROR TÉCNICO ---
              println(s"  Se encontraron $erroresTecnicos errores técnicos.")
              updateTriggerFlag(idTrigger, 32, jdbcUrl, props)
            } else {
              // --- CASO ÉXITO TOTAL ---
              println("Validaciones Técnicas superadas sin errores.")
              updateTriggerFlag(idTrigger, 2, jdbcUrl, props)
            }
          }

        } catch {
          case e: Exception =>
            println(s"ERROR DE SISTEMA: ${e.getMessage}")
            e.printStackTrace()
        }
      }

    } catch {
      case e: Exception => e.printStackTrace()
    }
    spark.stop()
  }

  // Función auxiliar para actualizar el flag en la BBDD
  def updateTriggerFlag(id: Int, newFlag: Int, url: String, props: Properties): Unit = {
    val conn = DriverManager.getConnection(
      url,
      props.getProperty("jdbc.user"),
      props.getProperty("jdbc.password")
    )

    val stmt = conn.prepareStatement("UPDATE public.trigger_control SET flag = ? WHERE id_trigger = ?")
    stmt.setInt(1, newFlag)
    stmt.setInt(2, id)
    stmt.executeUpdate()
    conn.close()
  }
}