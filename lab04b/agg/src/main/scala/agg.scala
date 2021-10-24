import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object agg{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Lab04")
      .getOrCreate()

    import spark.implicits._

    val dfInput = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", "vladimir_cherny")
      .load()

    var df = dfInput.selectExpr("CAST(value AS STRING)")

    val schema = StructType(Seq(
      StructField("category", StringType, true),
      StructField("event_type", StringType, true),
      StructField("item_id", StringType, true),
      StructField("item_price", StringType, true),
      StructField("timestamp", LongType, true),
      StructField("uid", StringType, true)
    ))

    df = df.withColumn("jsonData", from_json(col("value"), schema)).select("jsonData.*")

    df = df.withColumn("date", ($"timestamp" / 1000).cast(TimestampType))

    df = df.groupBy(window(col("date"), "1 hours"/*, "5 seconds"*/)).agg(
      sum(when(col("event_type") === "buy", col("item_price")).otherwise(0)).alias("revenue"),
      sum(when(col("uid").isNotNull, 1).otherwise(0)).alias("visitors"),
      sum(when(col("event_type") === "buy", 1).otherwise(0)).alias("purchases")
    )

    df = df.withColumn("aov", col("revenue")/col("purchases"))
    df = df.withColumn("start_ts", col("window.start").cast("long"))
    df = df.withColumn("end_ts", col("window.end").cast("long"))
    df = df.drop(col("window"))

    val query = df
      .selectExpr("CAST(start_ts AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("kafka")
      .option("checkpointLocation", "/tmp/vladimircherny1/chk")
      .option("kafka.bootstrap.servers", "10.0.0.5:6667")
      .option("topic", "vladimir_cherny_lab04b_out")
      .option("maxOffsetsPerTrigger", 200)
      .outputMode("update")
      .start()


    query.awaitTermination()

  }
}

