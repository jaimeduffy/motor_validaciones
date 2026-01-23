package org.yiyit

import org.apache.spark.sql.SparkSession
import java.util.Properties

object App {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ValidacionBigData")
      .master("local[*]")
      .getOrCreate()

    // Cargar el fichero application.properties
    val props = new Properties()
    val propertiesFile = getClass.getResourceAsStream("/application.properties")

    if (propertiesFile != null) {
      props.load(propertiesFile)
      println("Archivo de configuración cargado correctamente.")
    } else {
      println("ERROR: No se encuentra application.properties en resources.")
      System.exit(1)
    }

    // Extraer las variables
    val jdbcUrl = props.getProperty("jdbc.url")
    val connectionProps = new Properties()
    connectionProps.put("user", props.getProperty("jdbc.user"))
    connectionProps.put("password", props.getProperty("jdbc.password"))
    connectionProps.put("driver", props.getProperty("jdbc.driver"))

    println(s"Intentando conectar a: $jdbcUrl")

    // Probar conexión
    try {
      val df = spark.read.jdbc(jdbcUrl, "trigger_control", connectionProps)
      println("\nCONEXIÓN EXITOSA")
      df.printSchema()
    } catch {
      case e: Exception =>
        println("\nFALLO LA CONEXIÓN")
        println(s"Error: ${e.getMessage}")
    }
    spark.stop()
  }
}