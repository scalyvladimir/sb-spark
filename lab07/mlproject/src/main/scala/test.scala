import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object test{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Lab05")
      .getOrCreate()

    import spark.implicits._

    /*def killAll() = {
      SparkSession
        .active
        .streams
        .active
        .foreach { x =>
                    val desc = x.lastProgress.sources.head.description
                    x.stop
                    println(s"Stopped ${desc}")
        }               
    }*/

    val dfInput = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", "vladimir_cherny")
      .load()
      .selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("uid", StringType)
      .add("visits", ArrayType(new StructType()
        .add("url", StringType)
        .add("timestamp", LongType)))

    val dfUnpacked = dfInput
      .withColumn("jsonData", from_json(col("value"), schema))
      .select("jsonData.uid", "jsonData.visits")
      .withColumn("url", explode(col("visits.url")))
      .withColumn("domains", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("domains", regexp_replace(col("domains"), "www.", ""))
      .select("uid", "domains")
      .groupBy("uid").agg(collect_list("domains").as("domains"))

    val model = PipelineModel.load("log_reg")

    val dfPredict = model.transform(dfUnpacked)
      .select('uid, 'res)

    val query = dfPredict
      .selectExpr("CAST(uid AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("kafka")
      .option("checkpointLocation", "lab07-vvc")
      .option("kafka.bootstrap.servers", "10.0.0.5:6667")
      .option("topic", "vladimir_cherny_lab07_out")
      .option("maxOffsetsPerTrigger", 200)
      .outputMode("update")
      .start

    query.awaitTermination

    //killAll

    spark.stop

  }
}

