package org.yiyit

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import java.util.Properties // Gestiona archivos .properties
import java.sql.{Connection, DriverManager} // Gestiona conexiones JDBC con bases de datos
import org.yiyit.utils.DbLogger
import org.yiyit.models.ValidationError
import org.yiyit.validations.{TableValidator, TechnicalValidator, ReferentialValidator, FunctionalValidator}

object App {
  def main(args: Array[String]): Unit = {
    // Inicializamos el SparkSession
    val spark = SparkSession.builder()
      .appName("ValidacionBigData")
      .master("local[*]")
      .getOrCreate()
    // Configuración de logs
    spark.sparkContext.setLogLevel("ERROR")

    // Cargamos los parámetros para la conexión con la bbdd
    val props = new Properties()
    val propertiesFile = getClass.getResourceAsStream("/application.properties") // busca el archivo en el path
    if (propertiesFile != null) props.load(propertiesFile) else System.exit(1) // Cargamos el archivo en el objeto props

    val jdbcUrl = props.getProperty("jdbc.url")
    // connectionProps es el objeto Properties estandar de Spark
    val connectionProps = new Properties()
    connectionProps.put("user", props.getProperty("jdbc.user"))
    connectionProps.put("password", props.getProperty("jdbc.password"))
    connectionProps.put("driver", props.getProperty("jdbc.driver"))

    // Optimización: fetchsize para reducir roundtrips en lecturas JDBC de Spark
    connectionProps.put("fetchsize", "10000")

    println("\n--> INICIANDO MOTOR DE VALIDACIÓN ...")

    try {
      // Cargamos trigger_control y filtramos por "flag = 0" (acciones pendientes)
      val triggerDf = spark.read.jdbc(jdbcUrl, "public.trigger_control", connectionProps)
      val pendientes = triggerDf.filter("flag = 0").collect()

      if (pendientes.isEmpty) println("--> No hay tareas pendientes.")
      // Realizamos las validaciones para cada una de las tareas pendientes
      pendientes.foreach { row =>
        // Inicio Temporizador
        val tiempoInicio = System.nanoTime()
        val idTrigger = row.getAs[Int]("id_trigger")
        val tableName = row.getAs[String]("table_name")
        val idTypeTable = row.getAs[String]("id_type_table")

        println(s"\n======================================================")
        println(s"  PROCESANDO TRIGGER ID: $idTrigger")
        println(s"  Tabla: $tableName | id_type_table: $idTypeTable")
        println(s"========================================================")

        // Optimización: Abrimos una única conexión JDBC por trigger para reutilizarla en todos los updates de flag
        // Antes creaba una nueva conexión en la función updateTriggerFlag
        val conn = DriverManager.getConnection(jdbcUrl, props.getProperty("jdbc.user"), props.getProperty("jdbc.password"))
        try {
          // Cargar table_configuration para esa tabla
          val tableConfigDf = spark.read.jdbc(jdbcUrl, s"(SELECT * FROM public.table_configuration WHERE id_type_table = '$idTypeTable') as t", connectionProps)
          val hasHeader = tableConfigDf.first().getAs[Boolean]("header")
          println(s"--> table_configuration cargado: header = $hasHeader")

          // Cargar datos desde las tablas madres y contar los registros
          val dataDf = spark.read.jdbc(jdbcUrl, tableName, connectionProps)
          updateTriggerFlag(conn, idTrigger, 1)
          println("--> Flag actualizado a 1 (Ingesta OK)")

          // Cargar reglas desde semantic_layer ordenando por el campo field_position
          val semanticLayerDf = spark.read.jdbc(jdbcUrl, s"(SELECT * FROM public.semantic_layer WHERE id_type_table = '$idTypeTable' ORDER BY field_position) as r", connectionProps)
          val reglas = semanticLayerDf.collect()  // Uso collect porque sé que el Df es pequeño
          // Columnas esperadas
          val expectedColumns = reglas.map(_.getAs[String]("field_name")).toSeq
          // Columnas que forman parte de la PK
          val pkColumns = reglas.filter(r => Option(r.getAs[Boolean]("pk")).getOrElse(false)).map(_.getAs[String]("field_name")).toSeq

          // =======================
          // FASE 1: ESTRUCTURA
          // =======================
          // Empezamos las validaciones y actualizamos el flag a 11 para indicar que se está procesando
          updateTriggerFlag(conn, idTrigger, 11)
          println("--> Flag actualizado a 11 (Procesando...)")
          println("\n--> [FASE 1] Validando estructura...")

          // Validaciones de Tabla
          val estructuraResult = TableValidator.validateStructure(dataDf, expectedColumns, hasHeader)

          if (!estructuraResult.success) {
            // Caso error
            val errorMsg = estructuraResult.errorMessage.getOrElse("Error desconocido")
            println(s"   FALLO: $errorMsg")
            DbLogger.logError(props, idTrigger, tableName, "ALL_COLUMNS", errorMsg, "table_validation", "STRUCTURE_MISMATCH", "1")
            // Actualizar: FLAG a 31
            updateTriggerFlag(conn, idTrigger, 31)
            println("   Flag actualizado a 31")
          } else {
            // Caso Éxito
            println("   OK")
            // Tomamos el DF resultante y lo persistimos para reutilizarlo en las fases siguientes sin releer de JDBC
            val validatedDf = estructuraResult.dataFrame.get.persist(StorageLevel.MEMORY_AND_DISK)
            // Forzamos la materialización del cache
            // Calculamos el número de registros para actualizar el trigger_control
            val numRegistros = validatedDf.count()
            // Actualizar: FLAG a 12 y numRegistros
            updateTriggerFlag(conn, idTrigger, 12, numRegistros)
            println("--> Flag actualizado a 12")

            try {
              // =======================
              // FASE 2: VALIDACIONES TÉCNICAS
              // =======================
              var errores: List[ValidationError] = List.empty
              var faseActual = ""

              // Tipos de datos
              println("\n--> [FASE 2.1] Validando tipos de datos...")
              errores = TechnicalValidator.validateDataTypes(validatedDf, reglas)
              faseActual = "TIPO_DATOS"

              // Nulos
              if (errores.isEmpty) {
                println("   OK")
                println("\n--> [FASE 2.2] Validando nulos...")
                errores = TechnicalValidator.validateNulls(validatedDf, reglas)
                faseActual = "NULOS"
              }

              // Longitud
              if (errores.isEmpty) {
                println("   OK")
                println("\n--> [FASE 2.3] Validando longitud...")
                errores = TechnicalValidator.validateLengths(validatedDf, reglas)
                faseActual = "LONGITUD"
              }

              // Primary Key
              if (errores.isEmpty && pkColumns.nonEmpty) {
                println("   OK")
                println(s"\n--> [FASE 2.4] Validando PK (${pkColumns.mkString(", ")})...")
                errores = TechnicalValidator.validatePrimaryKey(validatedDf, pkColumns)
                faseActual = "PRIMARY_KEY"
              }

              if (errores.nonEmpty) {
                // Caso Error
                logErrors(errores, faseActual, props, idTrigger, tableName)
                updateTriggerFlag(conn, idTrigger, 32)
                println(s"\n   Flag actualizado a 32")
              } else {
                // Caso Éxito
                println("   OK")
                updateTriggerFlag(conn, idTrigger, 13)
                println("--> Flag actualizado a 13")

                // =======================
                // FASE 3: INTEGRIDAD REFERENCIAL
                // =======================
                println("\n--> [FASE 3] Validaciones de Integridad Referencial...")
                errores = ReferentialValidator.validate(validatedDf, reglas, jdbcUrl, connectionProps)
                faseActual = "INTEGRIDAD_REFERENCIAL"

                if (errores.nonEmpty) {
                  // Caso Error
                  logErrors(errores, faseActual, props, idTrigger, tableName)
                  updateTriggerFlag(conn, idTrigger, 33)
                  println(s"\n   Flag actualizado a 33")
                } else {
                  //Caso Éxito
                  println("   OK")
                  updateTriggerFlag(conn, idTrigger, 14)
                  println("--> Flag actualizado a 14")

                  // =======================
                  // FASE 4: VALIDACIONES FUNCIONALES
                  // =======================
                  println("\n--> [FASE 4] Validaciones funcionales...")
                  errores = FunctionalValidator.validate(validatedDf)
                  faseActual = "FUNCIONAL"

                  if (errores.nonEmpty) {
                    // Caso Error
                    logErrors(errores, faseActual, props, idTrigger, tableName)
                    updateTriggerFlag(conn, idTrigger, 34)
                    println(s"\n   Flag actualizado a 34")
                  } else {
                    // Caso éxito
                    println("   OK")
                    updateTriggerFlag(conn, idTrigger, 2)
                    println("\n--> Validación Completada - Flag actualizado a 2")
                  }
                }
              }
            } finally {
              // Liberamos el cache del DF al terminar con este trigger
              validatedDf.unpersist()
            }
          }
        } catch {
          case e: Exception =>
            println(s"\n   ERROR DE SISTEMA: ${e.getMessage}")
            updateTriggerFlag(conn, idTrigger, 3)
            DbLogger.logError(props, idTrigger, tableName, "SYSTEM", e.getMessage, "system_error", "EXECUTION_ERROR", "1")
        } finally {
          // Cerramos la conexión JDBC
          conn.close()
          // Fin timer y cálculo
          val tiempoFin = System.nanoTime()
          val duracionSegundos = (tiempoFin - tiempoInicio) / 1e9
          println(s"\n--> Tiempo de procesamiento: ${"%.2f".format(duracionSegundos)} segundos")
        }
      }
    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    }

    spark.stop()
    println("\n--> Motor de validación finalizado.")
  }

  // Función para registar todos los errores de una fase en la BBDD
  private def logErrors(errores: List[ValidationError], fase: String, props: Properties, idTrigger: Int, tableName: String): Unit = {
    println(s"   FALLO en $fase:")
    errores.foreach { error =>
      println(s"      ${error.errorMessage}")

      val codigoError = error.errorType.getOrElse(
        fase match {
          case "TIPO_DATOS" => "DATA_TYPE_ERROR"
          case "NULOS" => "NOT_NULL_ERROR"
          case "LONGITUD" => "LENGTH_ERROR"
          case "PRIMARY_KEY" => "PK_ERROR"
          case "INTEGRIDAD_REFERENCIAL" => "REF_INTEGRITY_ERROR"
          case "FUNCIONAL" => "FUNCTIONAL_ERROR"
          case _ => "VALIDATION_ERROR"
        }
      )
      DbLogger.logError(props, idTrigger, tableName, error.columnName, error.errorMessage, "functional_validation", codigoError, "1")
    }
  }

  // Actualiza la flag del registro en trigger_control
  // Reutiliza la conexión JDBC abierta para el trigger actual en lugar de crear una nueva cada vez
  private def updateTriggerFlag(conn: Connection, id: Int, newFlag: Int): Unit = {
    val stmt = conn.prepareStatement("UPDATE public.trigger_control SET flag = ? WHERE id_trigger = ?")
    try {
      stmt.setInt(1, newFlag)
      stmt.setInt(2, id)
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }

  // Sobrecarga del metodo para que acepte también el numero de registros para actualizar la columna row_count en trigger_control
  // Solo se llamará la primera vez para que no se actualice contínuamente el número de registros cada vez que actualicemos un flag
  private def updateTriggerFlag(conn: Connection, id: Int, newFlag: Int, count: Long): Unit = {
    val stmt = conn.prepareStatement("UPDATE public.trigger_control SET flag = ?, row_count = ? WHERE id_trigger = ?")
    try {
      stmt.setInt(1, newFlag)
      stmt.setLong(2, count)
      stmt.setInt(3, id)
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }
}