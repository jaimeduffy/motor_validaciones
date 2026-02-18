package org.yiyit.validations

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.yiyit.SparkSessionProvider

import java.util.Properties

class ReferentialValidatorTest extends AnyFunSuite with SparkSessionProvider {
  import spark.implicits._
  // Esquema de semantic_layer
  private val semanticSchema = StructType(Seq(
    StructField("id_type_table", StringType),
    StructField("field_position", IntegerType),
    StructField("field_name", StringType),
    StructField("field_description", StringType),
    StructField("pk", BooleanType),
    StructField("data_type", StringType),
    StructField("length", StringType),
    StructField("nullable", BooleanType),
    StructField("decimal_symbol", StringType),
    StructField("referential_bbdd", StringType),
    StructField("referential_table_field_name", StringType),
    StructField("init_date", TimestampType)
  ))

  private def buildIRRule(fieldName: String, refBbdd: String = null, refTableField: String = null): Row = {
    val row = Row(
      "T1",         // id_type_table
      null,          // field_position
      fieldName,     // field_name
      null,          // field_description
      false,         // pk
      "STRING",      // data_type
      null,          // length
      true,          // nullable
      null,          // decimal_symbol
      refBbdd,       // referential_bbdd
      refTableField, // referential_table_field_name
      null           // init_date
    )
    spark.createDataFrame(java.util.Collections.singletonList(row), semanticSchema).first()
  }

  // =========================================================================
  // Tests: sin reglas de integridad referencial
  // =========================================================================

  test("validate - sin columnas con referencia devuelve lista vacia") {
    val df = Seq(("T1", "Cover")).toDF("template_code", "sheet")
    val reglas = Array(
      buildIRRule("template_code", null, null),
      buildIRRule("sheet", null, null)
    )

    val errores = ReferentialValidator.validate(df, reglas, "jdbc:fake", new Properties())

    assert(errores.isEmpty)
  }

  test("validate - sin columnas con referencia (vacio) devuelve lista vacia") {
    val df = Seq("T1").toDF("template_code")
    val reglas = Array(
      buildIRRule("template_code", "", "")
    )

    val errores = ReferentialValidator.validate(df, reglas, "jdbc:fake", new Properties())

    assert(errores.isEmpty)
  }

  // =========================================================================
  // Tests: formato invalido en referential_table_field_name
  // =========================================================================

  test("validate - formato invalido en referential_table_field_name genera error") {
    val df = Seq("T1").toDF("template_code")
    val reglas = Array(
      buildIRRule("template_code", "bu_yiyit_bigdata", "formato_sin_punto")
    )

    val errores = ReferentialValidator.validate(df, reglas, "jdbc:fake", new Properties())

    assert(errores.nonEmpty)
    assert(errores.head.errorType.contains("REF_INTEGRITY_ERROR"))
  }

  // =========================================================================
  // Tests: validacion real con JDBC (usando H2 en memoria)
  // =========================================================================

  test("validate - valores existen en tabla de referencia (H2)") {
    val h2Url = "jdbc:h2:mem:test_ref_ok;DB_CLOSE_DELAY=-1"
    val h2Props = new Properties()
    h2Props.put("user", "sa")
    h2Props.put("password", "")
    h2Props.put("driver", "org.h2.Driver")

    val refDf = Seq("T1", "T2", "T3").toDF("TEMPLATE_CODE")
    refDf.write.mode("overwrite").jdbc(h2Url, "PUBLIC.TD_TEMPLATES", h2Props)

    val df = Seq(("T1", "Cover"), ("T2", "CCY")).toDF("template_code", "sheet")
    val reglas = Array(
      buildIRRule("template_code", "PUBLIC", "TD_TEMPLATES.template_code")
    )

    val errores = ReferentialValidator.validate(df, reglas, h2Url, h2Props)

    assert(errores.isEmpty)
  }

  test("validate - valores NO existen en tabla de referencia (H2)") {
    val h2Url = "jdbc:h2:mem:test_ref_fail;DB_CLOSE_DELAY=-1"
    val h2Props = new Properties()
    h2Props.put("user", "sa")
    h2Props.put("password", "")
    h2Props.put("driver", "org.h2.Driver")

    val refDf = Seq("T1", "T2").toDF("template_code")
    refDf.write.mode("overwrite").jdbc(h2Url, "PUBLIC.TD_TEMPLATES", h2Props)

    val df = Seq(("T1", "Cover"), ("T99", "CCY")).toDF("template_code", "sheet")
    val reglas = Array(
      buildIRRule("template_code", "PUBLIC", "TD_TEMPLATES.template_code")
    )

    val errores = ReferentialValidator.validate(df, reglas, h2Url, h2Props)

    assert(errores.nonEmpty)
    assert(errores.head.errorMessage.contains("1"))
    assert(errores.head.columnName == "template_code")
  }

  // =========================================================================
  // Tests: error de conexion genera error controlado
  // =========================================================================

  test("validate - error de conexion JDBC genera error controlado") {
    val df = Seq("T1").toDF("template_code")
    val reglas = Array(
      buildIRRule("template_code", "bd_inexistente", "tabla.columna")
    )
    val badProps = new Properties()
    badProps.put("driver", "org.postgresql.Driver")

    val errores = ReferentialValidator.validate(df, reglas, "jdbc:postgresql://host_falso:5432/db", badProps)

    assert(errores.nonEmpty)
    assert(errores.head.errorMessage.contains("Error al validar"))
  }
}