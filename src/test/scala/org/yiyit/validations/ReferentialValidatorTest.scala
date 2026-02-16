package org.yiyit.validations

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.yiyit.SparkSessionProvider

import java.util.Properties

class ReferentialValidatorTest extends AnyFunSuite with SparkSessionProvider {
  import spark.implicits._
  // =========================================================================
  // Helper: construir Array[Row] simulando reglas de semantic_layer con campos de IR
  // =========================================================================
  private val ruleSchema = StructType(Seq(
    StructField("field_name", StringType),
    StructField("referential_bbdd", StringType),
    StructField("referential_table_field_name", StringType)
  ))

  private def buildIRRule(fieldName: String, refBbdd: String = null, refTableField: String = null): Row = {
    Row(fieldName, refBbdd, refTableField)
  }

  // =========================================================================
  // Tests: sin reglas de integridad referencial
  // =========================================================================

  test("validate - sin columnas con referencia devuelve lista vacía") {
    // Arrange
    val df = Seq(("T1", "Cover")).toDF("template_code", "sheet")
    val reglas = Array(
      buildIRRule("template_code", null, null),
      buildIRRule("sheet", null, null)
    )

    // Act
    val errores = ReferentialValidator.validate(df, reglas, "jdbc:fake", new Properties())

    // Assert
    assert(errores.isEmpty)
  }

  test("validate - sin columnas con referencia (vacío) devuelve lista vacía") {
    // Arrange
    val df = Seq("T1").toDF("template_code")
    val reglas = Array(
      buildIRRule("template_code", "", "")
    )

    // Act
    val errores = ReferentialValidator.validate(df, reglas, "jdbc:fake", new Properties())

    // Assert
    assert(errores.isEmpty)
  }

  // =========================================================================
  // Tests: formato inválido en referential_table_field_name
  // =========================================================================

  test("validate - formato inválido en referential_table_field_name genera error") {
    // Arrange
    val df = Seq("T1").toDF("template_code")
    val reglas = Array(
      buildIRRule("template_code", "bu_yiyit_bigdata", "formato_sin_punto") // falta el "."
    )

    // Act
    val errores = ReferentialValidator.validate(df, reglas, "jdbc:fake", new Properties())

    // Assert
    assert(errores.nonEmpty)
    assert(errores.head.errorMessage.contains("Formato inválido"))
    assert(errores.head.errorType.contains("REF_INTEGRITY_ERROR"))
  }

  // =========================================================================
  // Tests: validación real con JDBC (usando H2 en memoria)
  // =========================================================================

  test("validate - valores existen en tabla de referencia (H2)") {
    // Arrange: crear tabla de referencia en H2
    val h2Url = "jdbc:h2:mem:test_ref_ok;DB_CLOSE_DELAY=-1"
    val h2Props = new Properties()
    h2Props.put("user", "sa")
    h2Props.put("password", "")
    h2Props.put("driver", "org.h2.Driver")

    // Crear tabla de referencia con Spark
    val refDf = Seq("T1", "T2", "T3").toDF("template_code")
    refDf.write.mode("overwrite").jdbc(h2Url, "public.td_templates", h2Props)

    // DF a validar: todos los template_code existen en la referencia
    val df = Seq(("T1", "Cover"), ("T2", "CCY")).toDF("template_code", "sheet")
    val reglas = Array(
      buildIRRule("template_code", "public", "td_templates.template_code")
    )

    // Act
    val errores = ReferentialValidator.validate(df, reglas, h2Url, h2Props)

    // Assert
    assert(errores.isEmpty)
  }

  test("validate - valores NO existen en tabla de referencia (H2)") {
    // Arrange
    val h2Url = "jdbc:h2:mem:test_ref_fail;DB_CLOSE_DELAY=-1"
    val h2Props = new Properties()
    h2Props.put("user", "sa")
    h2Props.put("password", "")
    h2Props.put("driver", "org.h2.Driver")

    val refDf = Seq("T1", "T2").toDF("template_code")
    refDf.write.mode("overwrite").jdbc(h2Url, "public.td_templates", h2Props)

    // DF a validar: "T99" no existe en la referencia
    val df = Seq(("T1", "Cover"), ("T99", "CCY")).toDF("template_code", "sheet")
    val reglas = Array(
      buildIRRule("template_code", "public", "td_templates.template_code")
    )

    // Act
    val errores = ReferentialValidator.validate(df, reglas, h2Url, h2Props)

    // Assert
    assert(errores.nonEmpty)
    assert(errores.head.errorMessage.contains("1")) // 1 valor no existe
    assert(errores.head.columnName == "template_code")
  }

  // =========================================================================
  // Tests: error de conexión genera error controlado
  // =========================================================================

  test("validate - error de conexión JDBC genera error controlado") {
    // Arrange
    val df = Seq("T1").toDF("template_code")
    val reglas = Array(
      buildIRRule("template_code", "bd_inexistente", "tabla.columna")
    )
    val badProps = new Properties()
    badProps.put("driver", "org.postgresql.Driver")

    // Act
    val errores = ReferentialValidator.validate(df, reglas, "jdbc:postgresql://host_falso:5432/db", badProps)

    // Assert
    assert(errores.nonEmpty)
    assert(errores.head.errorMessage.contains("Error al validar"))
  }
}
