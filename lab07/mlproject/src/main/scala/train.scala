import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.{Pipeline, PipelineModel}

object train{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Lab05")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .read
      .option("header", true)
      .json("/labs/laba07/laba07.json")
      .select(col("*"), explode(col("visits")))
      .select('uid, 'gender_age, col("col.*"))
      .na.drop(List("uid"))
      .withColumn("url", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("url", regexp_replace(col("url"), "www.", ""))
      .toDF

    val train_df = df
      .groupBy("uid", "gender_age")
      .agg(collect_list('url).as("domains"))

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
            .setInputCol("gender_age")
            .setOutputCol("label")

    val labels = indexer.fit(train_df).labels

    val lr = new LogisticRegression()
            .setMaxIter(10)
            .setRegParam(0.001)

    val revIndexer = new IndexToString()
            .setInputCol("prediction")
            .setLabels(labels)
            .setOutputCol("res")

    val pipeline = new Pipeline()
          .setStages(Array(cv, indexer, lr, revIndexer))

    val model = pipeline.fit(train_df)

    model.write.overwrite().save("log_reg")

    spark.stop

  }
}

