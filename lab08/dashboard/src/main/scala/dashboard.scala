import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.{PipelineModel}
import org.elasticsearch.spark.sql
import org.apache.spark.sql.functions._

object dashboard{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Lab05")
      .getOrCreate()

    import spark.implicits._

    val elastic_node = "10.0.0.5:9200"
    val elastic_user = "vladimir.cherny"
    val elastic_pass = "ZyqQ3Mdj"

    val dfInput = spark
      .read
      .option("header", true)
      .json("/labs/laba08/laba08.json")
      .select(col("*"), explode(col("visits")))
      .select('uid, 'date, col("col.url"))
      .na.drop(List("uid"))
      .withColumn("url", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("url", regexp_replace(col("url"), "www.", ""))
      .toDF

    val dfGroupped = dfInput
      .groupBy("uid", "date").agg(collect_list("url").as("domains"))

    val model = PipelineModel.load("lab07_model")

    val dfPredict = model.transform(dfGroupped)
      .withColumnRenamed("res", "gender_age")
      .select('uid, 'gender_age, 'date)

    val esOptions = Map(
      "es.nodes" -> elastic_node,
      "es.batch.write.refresh" -> "false",
      "es.net.http.auth.user" -> elastic_user,
      "es.net.http.auth.pass" -> elastic_pass,
      "es.nodes.wan.only" -> "true"
    )

    dfPredict
      .write
      .mode("append")
      .format("org.elasticsearch.spark.sql")
      .options(esOptions)
      .save("vladimir_cherny_lab08/_doc")

    spark.stop

  }
}

