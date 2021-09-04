package resource

import com.typesafe.config.Config
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import scala.io.Source

object ingestSchema {

  def apply(config:Config):StructType = ingestSchema(config)

  def ingestSchema(config:Config): StructType = {

    val schemaPath = System.getProperty("user.dir") + "\\src\\main\\scala\\resource\\schema\\"
    val schemaFile = config.getString("schemaFile")
    val in         = Source.fromFile(schemaPath + schemaFile + ".csv").getLines()
    var schema     = new StructType()

    for(i <- in){
        schema = schema
          .add(i.split(",")(0), datatype(i.split(",")(1)), i.split(",")(2).toBoolean)
    }
    schema
  }
  def datatype(typ: String) ={
    typ match{
      case "string" => StringType
      case "int"    => IntegerType
    }
  }
}
