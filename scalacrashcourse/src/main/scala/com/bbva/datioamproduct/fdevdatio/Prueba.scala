package com.bbva.datioamproduct.fdevdatio

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}

object Prueba extends App {

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

  val spark = SparkSession.builder.master("local[*]").appName("Spark Session").getOrCreate()
  import spark.implicits._

  val list = List(1,2,3,4,5)
  val df = spark.sparkContext.parallelize(list).toDF("ColumnaA")
  val udfaClass = new UdfAMediaArmonica
  df.select(col("ColumnaA").cast("int").as("ColumnaA")).agg(udfaClass(col("ColumnaA"))).show




}
