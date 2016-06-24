/**
  * Created by xiangnanren on 23/06/16.
  */

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


class LubmConfig(sc:SparkContext) {

  val file: String = "/Users/xiangnanren/" +
    "IDEAWorkspace/lubm-generator/data/lubm1_encoded.txt"
  val sameAsUpBound: Int = 8
  val sameAsLowBound: Int = 4
  val transitUpBound: Int = 10
  val transitLowBound: Int = 5
  val df: DataFrame = Lubm2DF(sc)
  val maxIndividual: Long = getMaxIndividual(df)


  def Lubm2DF(sc: SparkContext): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val schemaString = "s p o"
    val schema = StructType(
      schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true: Boolean)))

    val rowRDD = sc.textFile(file)
      .map(_.replace("(","")
        .replace(")","")
        .split(","))
      .map(p => Row(p(0), p(1), p(2).trim))
    sqlContext.createDataFrame(rowRDD, schema)
  }

  def getMaxIndividual(df: DataFrame): Long  = {
    val maxIndividu1 = df.where(df("p") !== "0")
      .select(df("s").cast(LongType), df("o").cast(LongType))
      .agg(max("s"),max("o")).first

    val maxIndividu2 =  df.where(df("p") <=> "0")
      .select(df("s").cast(LongType)).agg(max("s")).first


    maxIndividu1.getLong(0) max maxIndividu1.getLong(1) max maxIndividu2.getLong(0)

    }
}


object LubmConfig {
  def apply(sc: SparkContext): LubmConfig = new LubmConfig(sc)
}
