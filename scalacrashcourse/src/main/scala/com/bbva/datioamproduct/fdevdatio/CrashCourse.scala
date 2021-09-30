package com.bbva.datioamproduct.fdevdatio

import java.net.URI
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.schema.DatioSchema
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigException
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Main entrypoint for CrashCourse process.
  * Implements SparkProcess so it can be found by the Dataproc launcher using JSPI.
  *
  * Configuration for this class should be expressed in HOCON like this:
  *
  * CrashCourse {
  *   ...
  * }
  *
  * This example app reads and writes a csv file contained within the project, for external
  * input or output set the environment variables INPUT_PATH or OUTPUT_PATH, or modify the
  * proper configuration paths.
  *
  */
class CrashCourse extends SparkProcess {

  private val logger = LoggerFactory.getLogger(classOf[CrashCourse])

  override def getProcessId: String = "CrashCourse"

  override def runProcess(context: RuntimeContext): Int = {
    val OK = 0
    val ERR = 1
    Try{
      /*
      val datioSparkSession = DatioSparkSession.getOrCreate()
      val sc = datioSparkSession.getSparkSession.sparkContext
      val list1 = List(("camino",23), ("casa",13), ("comida",14),("salida",43))
      val list2 = List((23,"complemento camino"), (23,"complemento adicional camino"), (13,"casa de 1 piso con 2 habitaciones"), (14,"hamburguesa con papas"),
      (43,"emergencia"),(43,"principal"), (54, "PRUEBA"))
      val rdd1 = sc.parallelize(list1)
      val rdd2 = sc.parallelize(list2)
      val rddOrdenado = rdd1.keyBy(_._2)//rdd1.map(x => (x._2, x._1))
      rdd2.join(rddOrdenado).foreach(println)
      println("LEFT")
      rdd2.leftOuterJoin(rddOrdenado).foreach(println)
*/


      /*
      val inputDs = datioSparkSession.getSparkSession.sparkContext.textFile("C:/Users/Dell/Documents/Datio/Certificacion/wjeew_datio_scala_crash_course/scalacrashcourse/src/main/resources/data/green_eggs_and_ham.txt")
      val otraCosa = inputDs.flatMap(a => a.split(" "))
      val respuesta = otraCosa.map((_,1)).filter(_._1.nonEmpty).reduceByKey(_ + _)
      respuesta.coalesce(1).sortBy(_._2).foreach(println)


      val listaA = List(1,2,3,4,5,6)
      val respuesta = for {
        x <- listaA
        y <- listaA
      }yield (x,y)
      respuesta.foreach(println)
      println(s"Cantidad de combinaciones: ${respuesta.size}")

      val sides = 6
      val sc = datioSparkSession.getSparkSession.sparkContext
      val rddResp = sc.parallelize(respuesta)
      val mapSum = rddResp.map( x=> (x._1 + x._2,1))
      mapSum.foreach(println)
      val rddCount = mapSum.reduceByKey(_+_)

      val rddCountBySumWithProb = rddCount.map(e => (e._1, e._2, e._2 / (sides * sides).toDouble))
      rddCountBySumWithProb.foreach(println)
      rddCountBySumWithProb.coalesce(1).sortBy(_._1,false).foreach(println)


      //val df = datioSparkSession.getSparkSession.sparkContext.parallelize(List(1,2,3,4,5))
      //println(df.map(x=> x * 2).reduce(_ + _))

      val data = Seq(        ("3!", 2),
        ("3!", 3),
        ("4!", 2),
        ("4!", 3),
        ("4!", 4),
        ("5!", 2),
        ("5!", 3),
        ("5!", 4),
        ("5!", 5)
      )

      //val rddData = datioSparkSession.getSparkSession.sparkContext.parallelize(data)

      //rddData.foreach(println)
      //rddData.reduceByKey(_ * _).foreach(println)

*/


      val datioSparkSession = DatioSparkSession.getOrCreate()
      val sc = datioSparkSession.getSparkSession.sparkContext
      val spark = SparkSession.builder.master("local[*]").appName("Spark Session").getOrCreate()
      import spark.implicits._
      /*
      val list = List(("string1", "string2", "string3", 1), ("string1", null, "string3", 2), ("string1", null, "string3", 3),
            ("string1", "string2", "string3", 4), ("string1", "string2", null, 5), ("string1", "string2", "FLAG", 6),
            ("string1", "string2", "string3", 7), (null, "FLAG2", "string3", 8))
            val df = sc.parallelize(list).toDF("ColumnaA", "ColumnaB", "ColumnaC", "ColumnaD")

            val udfSuma = (x:Int) => x + 10

            def transformar(cadena: String):String = {
              cadena match {
                case "FLAG" => "SUST1"
                case "FLAG2" => "SUST2"
                case _ => cadena
              }
            }

            def concatenar(columnaA: String, columnaB: String, columnaC: String): String = {
              columnaA + columnaB + columnaC
            }


            val udfReplace = udf(transformar(_: String))
            val defineUDF = udf(udfSuma)
            val defineUDFConcat = udf(concatenar(_: String, _: String, _: String))

            df.select($"*", defineUDF(col("ColumnaD")),
              udfReplace(col("ColumnaB")), udfReplace(col("ColumnaC")),
              defineUDFConcat(col("ColumnaA"),col("ColumnaB"), col("ColumnaC"))).show()


      */

      val list = List(1,2,3,4,5)
      val df = sc.parallelize(list).toDF("ColumnaA")
      val udfaClass = new UdfAMediaArmonica
      df.select(col("ColumnaA").cast("int").as("ColumnaA")).agg(udfaClass(col("ColumnaA"))).show


    }match {
      case Success(_) =>
        logger.info("Successful processing!")
        OK
      case Failure(e) =>
        logger.error("There was an error during the processing of the data", e)
        ERR
    }


0
    /*
    Try {
      logger.info(s"Process Id: ${context.getProcessId}")
      // The DatioSparkSession should be the entrypoint for spark reading/writing
      val datioSparkSession = DatioSparkSession.getOrCreate()

      val jobConfig = context.getConfig.getConfig("CrashCourse").resolve()

      // Schemas can be specified both for reading and writing, please read the docs to find more
      // about their usage (schema of dataset, encryption/description)
      val inputSchemaPath = jobConfig.getString("inputSchemaPath")
      val outputSchemaPath = jobConfig.getString("outputSchemaPath")
      val inputSchema = DatioSchema.getBuilder.fromURI(URI.create(inputSchemaPath)).build()
      val outputSchema = DatioSchema.getBuilder.fromURI(URI.create(outputSchemaPath)).build()

      // Here is a simple logic that showcases reading and writing using a schema
      val spark = datioSparkSession.getSparkSession
      import spark.implicits._

      val inputPath = jobConfig.getString("inputPath")
      val inputDs = datioSparkSession.read.datioSchema(inputSchema).csv(inputPath)

      val totalMedalsDs = inputDs
          .select($"country",
                  $"total_summer" +
                  $"total_winter" +
                  $"gold_games" +
                  $"silver_games" +
                  $"bronze_games" as "total")

      // Write resulting dataset to console or to a new CSV file
      val outputPath = jobConfig.getString("outputPath")
      if (Option(outputPath).getOrElse("").trim.isEmpty) {
        totalMedalsDs.show(false)
      }
      else {
        outputSchema.validate(totalMedalsDs)
        datioSparkSession.write.mode(SaveMode.Overwrite).datioSchema(outputSchema).csv(totalMedalsDs, outputPath)
      }
    } match {
      case Success(_) =>
        logger.info("Successful processing!")
        OK
      case Failure(e) =>
        // You can do whatever exception control you see fit, keep in mind that if the exception
        // bubbles up, it will also be caught at launcher level and the process will return with error
        logger.error("There was an error during the processing of the data", e)
        ERR
    }
     */
  }
}

class UdfAMediaArmonica extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(
    StructField("ColumnaA", IntegerType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("numerador", IntegerType) ::
      StructField("denominador", DoubleType)
      :: Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
    buffer(1) = 0.00
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val div: Double = 1 / input.getInt(0).toDouble
    buffer(0) = buffer.getInt(0) + 1
    buffer(1) = buffer.getDouble(1) + div
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getInt(0) / buffer.getDouble(1)
  }
}