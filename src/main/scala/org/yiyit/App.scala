package org.yiyit

import org.apache.spark.sql.SparkSession
import java.util.Properties
import java.sql.DriverManager
import org.yiyit.utils.DbLogger
import org.yiyit.validations.{TableValidator, TechnicalValidator, ReferentialValidator, FunctionalValidator}

object App {
  def main(args: Array[String]): Unit = {
    // Inicializamos el SparkSession
    val spark = SparkSession.builder()
      .appName("ValidacionBigData")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Cargamos los parámetros para la conexión con la bbdd
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
      // Cargamos trigger_control y filtramos por "flag = 0" (Acciones pendientes)
      val triggerDf = spark.read.jdbc(jdbcUrl, "public.trigger_control", connectionProps)
      val pendientes = triggerDf.filter("flag = 0").collect()

      if (pendientes.isEmpty) println("--> No hay tareas pendientes.")
      // Realizamos las validaciones para cada una de las tareas pendientes
      pendientes.foreach { row =>
        val idTrigger = row.getAs[Int]("id_trigger")
        val tableName = row.getAs[String]("table_name")
        val idTypeTable = row.getAs[String]("id_type_table")

        println(s"\n========================================================")
        println(s"  PROCESANDO TRIGGER ID: $idTrigger")
        println(s"  Tabla: $tableName | id_type_table: $idTypeTable")
        println(s"========================================================")

        try {
          // Cargar table_configuration para esa tabla
          val configDf = spark.read.jdbc(jdbcUrl, s"(SELECT * FROM public.table_configuration WHERE id_type_table = '$idTypeTable') as t", connectionProps)
          val hasHeader = configDf.first().getAs[Boolean]("header")
          println(s"--> Configuración cargada: header = $hasHeader")

          // Cargar datos desde las tablas madres y contar los registros
          val dataDf = spark.read.jdbc(jdbcUrl, tableName, connectionProps)
          // Calculamos el número de registros para actualizar el trigger_control
          val numRegistros = dataDf.count()
          if (numRegistros > 0) {
            updateTriggerFlag(idTrigger, 1, numRegistros, jdbcUrl, props)
            println("--> Flag actualizado a 1 (Ingesta OK)")
            println(s"--> Datos cargados: $numRegistros registros")
          }

          // Cargar reglas desde semantic_layer ordenando por el campo field_position
          val rulesDf = spark.read.jdbc(jdbcUrl, s"(SELECT * FROM public.semantic_layer WHERE id_type_table = '$idTypeTable' ORDER BY field_position) as r", connectionProps)
          val reglas = rulesDf.collect()
          val expectedColumns = reglas.map(_.getAs[String]("field_name")).toSeq
          val pkColumns = reglas.filter(r => Option(r.getAs[Boolean]("pk")).getOrElse(false)).map(_.getAs[String]("field_name")).toSeq

          // =======================
          // FASE 1: ESTRUCTURA
          // =======================
          // Empezamos las validaciones y actualizamos el flag a 11 para indicar que se está procesando
          updateTriggerFlag(idTrigger, 11, numRegistros, jdbcUrl, props)
          println("--> Flag actualizado a 11 (Procesando...)")
          println("\n--> [FASE 1] Validando estructura...")

          val estructuraResult = TableValidator.validateStructure(dataDf, expectedColumns, hasHeader)

          if (!estructuraResult.success) {
            val errorMsg = estructuraResult.errorMessage.getOrElse("Error desconocido")
            println(s"   FALLO: $errorMsg")
            DbLogger.logError(props, idTrigger, tableName, "ALL_COLUMNS", errorMsg, "table_validation", "STRUCTURE_MISMATCH", "1")
            updateTriggerFlag(idTrigger, 31, jdbcUrl, props)
            println("   Flag actualizado a 31")
          } else {
            println("   OK")
            updateTriggerFlag(idTrigger, 12, jdbcUrl, props)
            println("--> Flag actualizado a 12")

            val validatedDf = estructuraResult.dataFrame.get

            // =======================
            // FASE 2: VALIDACIONES TÉCNICAS
            // =======================
            var errores: List[String] = List.empty
            var faseActual = ""

            // 2.1 Tipos de datos
            println("\n--> [FASE 2.1] Validando tipos de datos...")
            errores = TechnicalValidator.validateDataTypes(validatedDf, reglas)
            faseActual = "TIPO_DATOS"

            // 2.2 Nulos
            if (errores.isEmpty) {
              println("   OK")
              println("\n--> [FASE 2.2] Validando nulos...")
              errores = TechnicalValidator.validateNulls(validatedDf, reglas)
              faseActual = "NULOS"
            }

            // 2.3 Longitud
            if (errores.isEmpty) {
              println("   OK")
              println("\n--> [FASE 2.3] Validando longitud...")
              errores = TechnicalValidator.validateLengths(validatedDf, reglas)
              faseActual = "LONGITUD"
            }

            // 2.4 Primary Key
            if (errores.isEmpty && pkColumns.nonEmpty) {
              println("   OK")
              println(s"\n--> [FASE 2.4] Validando PK (${pkColumns.mkString(", ")})...")
              errores = TechnicalValidator.validatePrimaryKey(validatedDf, pkColumns)
              faseActual = "PRIMARY_KEY"
            }

            if (errores.nonEmpty) {
              logErrors(errores, faseActual, pkColumns.toList, props, idTrigger, tableName)
              updateTriggerFlag(idTrigger, 32, jdbcUrl, props)
              println(s"\n   Flag actualizado a 32")
            } else {
              println("   OK")
              updateTriggerFlag(idTrigger, 13, jdbcUrl, props)
              println("--> Flag actualizado a 13")

              // =======================
              // FASE 3: INTEGRIDAD REFERENCIAL
              // =======================
              println("\n--> [FASE 3] Validando integridad referencial...")
              errores = ReferentialValidator.validate(validatedDf, reglas, jdbcUrl, connectionProps)
              faseActual = "INTEGRIDAD_REFERENCIAL"

              if (errores.nonEmpty) {
                logErrors(errores, faseActual, pkColumns.toList, props, idTrigger, tableName)
                updateTriggerFlag(idTrigger, 33, jdbcUrl, props)
                println(s"\n   Flag actualizado a 33")
              } else {
                println("   OK")
                updateTriggerFlag(idTrigger, 14, jdbcUrl, props)
                println("--> Flag actualizado a 14")

                // =======================
                // FASE 4: VALIDACIONES FUNCIONALES
                // =======================
                println("\n--> [FASE 4] Validando reglas funcionales...")
                errores = FunctionalValidator.validate(validatedDf)
                faseActual = "FUNCIONAL"

                if (errores.nonEmpty) {
                  logErrors(errores, faseActual, pkColumns.toList, props, idTrigger, tableName)
                  updateTriggerFlag(idTrigger, 34, jdbcUrl, props)
                  println(s"\n   Flag actualizado a 34")
                } else {
                  println("   OK")
                  updateTriggerFlag(idTrigger, 2, jdbcUrl, props)
                  println("\n   VALIDACIÓN COMPLETADA - Flag actualizado a 2")
                }
              }
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

  private def logErrors(errores: List[String], fase: String, pkColumns: List[String], props: Properties, idTrigger: Int, tableName: String): Unit = {
    println(s"   FALLO en $fase:")
    errores.foreach { msg =>
      println(s"      $msg")
      val codigoError = fase match {
        case "TIPO_DATOS" => "DATA_TYPE_ERROR"
        case "NULOS" => "NOT_NULL_ERROR"
        case "LONGITUD" => "LENGTH_ERROR"
        case "PRIMARY_KEY" => "PK_ERROR"
        case "INTEGRIDAD_REFERENCIAL" => "REF_INTEGRITY_ERROR"
        case "FUNCIONAL" => "FUNCTIONAL_ERROR"
        case _ => "VALIDATION_ERROR"
      }
      val colName = if (msg.contains("Columna '")) msg.split("'")(1)
      else if (msg.contains("Clave primaria")) pkColumns.mkString(",")
      else if (msg.contains("data_name")) "data_name"
      else if (msg.contains("column_x")) "column_x"
      else if (msg.contains("excel_cell")) "excel_cell"
      else "FUNCTIONAL"
      DbLogger.logError(props, idTrigger, tableName, colName, msg, "functional_validation", codigoError, "1")
    }
  }

// Actualiza la flag del registro en trigger_control
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

  // Sobrecarga del metodo para que acepte también el numero de registros para actualizar la columna row_count en trigger_control
  // Solo se llamará la primera vez para que no se actualice contínuamente el número de registros cada vez que actualicemos un flag
  private def updateTriggerFlag(id: Int, newFlag: Int, count: Long, url: String, props: Properties): Unit = {
    val conn = DriverManager.getConnection(url, props.getProperty("jdbc.user"), props.getProperty("jdbc.password"))
    try {
      val stmt = conn.prepareStatement("UPDATE public.trigger_control SET flag = ?, row_count = ? WHERE id_trigger = ?")
      stmt.setInt(1, newFlag)
      stmt.setLong(2, count)
      stmt.setInt(3, id)
      stmt.executeUpdate()
      stmt.close()
    } finally {
      conn.close()
    }
  }
}