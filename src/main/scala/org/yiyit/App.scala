package org.yiyit

import org.apache.spark.sql.SparkSession
import java.util.Properties

object App {
  def main(args: Array[String]): Unit = {

    // Iniciamos la sesión de Spark
    val spark = SparkSession.builder()
      .appName("ValidacionBigData")
      .master("local[*]")
      .getOrCreate()

    // Cargamos el archivo de configuración (application.properties)
    val url = getClass.getResource("/application.properties")
    if (url == null) {
      println("ERROR: No se ha encontrado el archivo application.properties en resources")
      System.exit(1)
    }
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("/application.properties"))

    // Probamos la conexión leyendo la tabla 'trigger_control'
    println("--- INICIANDO PRUEBA DE CONEXIÓN A POSTGRESQL ---")

    try {
      // Leemos la tabla usando los datos del properties
      val dfTrigger = spark.read.jdbc(
        properties.getProperty("jdbc.url"),
        "trigger_control",
        properties
      )

      println("¡CONEXIÓN EXITOSA!")
      println("Mostrando esquema de la tabla trigger_control:")
      dfTrigger.printSchema()

    } catch {
      case e: Exception =>
        println("\nERROR DE CONEXIÓN:")
        println(e.getMessage)
        println("Revisa que el Docker esté encendido y el puerto sea correcto.")
    }

    spark.stop()
  }
}
