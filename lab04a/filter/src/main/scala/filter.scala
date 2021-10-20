import org.apache.spark.sql.functions._
//import sys.process._

object filter extends App {

  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> "lab04_input_data"
  )

  val df = spark.read.format("kafka").options(kafkaParams).load

  val jsonString = df
    .sample(0.4)
    .select('value.cast("string"))
    .as[String]

  val parsed = spark
    .read
    .json(jsonString)
    .withColumn("date", to_date(from_unixtime('timestamp / 1000)))
    .withColumn("date", from_unixtime(unix_timestamp('date, "yyyy-MM-dd"), "yyyyMMdd"))
    .withColumn("p_date", col("date"))

  val views = parsed.filter('event_type === "view")
  val purchases = parsed.filter('event_type === "buy")

  val PARTITION_KEY = "p_date"

  views
    .write
    .mode("overwrite")
    .partitionBy(PARTITION_KEY)
    .json("visits/view/")

  purchases
    .write
    .mode("overwrite")
    .partitionBy(PARTITION_KEY)
    .json("visits/buy/")

  //"!hdfs dfs -rm -r -f /user/vladimir.cherny/visits/view/_SUCCESS".!!
  //"!hdfs dfs -rm -r -f /user/vladimir.cherny/visits/buy/_SUCCESS".!!

}

