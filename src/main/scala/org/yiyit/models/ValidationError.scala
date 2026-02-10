package org.yiyit.models

// Case Class para el manejo de errores en las validaciones
case class ValidationError(
                            columnName: String,
                            errorMessage: String,
                            errorType: Option[String] = None
                          )