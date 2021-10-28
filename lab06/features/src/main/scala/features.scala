import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.DataFrame

object features{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Lab05")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val USER = "vladimir.cherny"
    val HDFS_DIR = s"/user/$USER/users-items/20200429"
    val OUT_DIR = s"/user/$USER/features"

    val input = spark.read
      .option("header",true)
      .json("/labs/laba03/weblogs.json").toDF
      .select('uid, explode(col("visits")))
      .select('uid, col("col.*"))
      .toDF

    val webLogs = input
      .na.drop(List("uid"))
      .withColumn("url", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("url", regexp_replace(col("url"), "www.", ""))
      .withColumn("url", regexp_replace(col("url"), "[.]", "-"))
      .na.drop(List("url"))

    val top_domains = webLogs
      .groupBy('url)
      .count
      .na.drop(List("url"))
      .orderBy('count.desc)
      .limit(1000)
      .orderBy('url.asc)
      .select('url)
      .rdd.map(r => r(0)).collect

    val topWebLogs = webLogs.filter('url.isInCollection(top_domains))

    val topWebMatrix = topWebLogs
      .groupBy("uid", "url")
      .count
      .groupBy("uid")
      .pivot("url")
      .sum("count")
      .na.fill(0)

    val col_arr = topWebMatrix.columns.filter(_ != "uid")
    val topDomainFeatures = topWebMatrix.select('uid, array(col_arr.map(col):_*).as("domain_features"))

    val uid_top_visitors = topDomainFeatures.select('uid).rdd.map(r => r(0)).collect

    val inferriorDomainFeatures = webLogs
      .select('uid)
      .filter(!col("uid").isInCollection(uid_top_visitors))
      .dropDuplicates("uid")
      .withColumn("domain_features", array(lit(0).cast(LongType)))

    val domainFeatures = topDomainFeatures.union(inferriorDomainFeatures).withColumnRenamed("uid", "uid1")

    val day_pivot_order = Seq("web_day_mon", "web_day_tue", "web_day_wed", "web_day_thu", "web_day_fri", "web_day_sat", "web_day_sun")

    val daysWebMatrix = webLogs.withColumn("timestamp",
      to_timestamp(col("timestamp")))
      .withColumn("day_of_week", concat(lit("web_day_"), lower(date_format(col("timestamp"), "E"))))
      .drop("timestamp")
      .groupBy("uid", "day_of_week")
      .count
      .groupBy("uid")
      .pivot("day_of_week", day_pivot_order)
      .sum("count")
      .na.fill(0)
      .withColumnRenamed("uid", "uid_days")

    val hour_pivot_order = Seq.range(0, 24).map(x => "web_hour_" + x.toString)

    val hoursWebMatrix = webLogs.withColumn("timestamp",
      to_timestamp(col("timestamp")))
      .withColumn("hour", concat(lit("web_hour_"), hour(col("timestamp"))))
      .drop("timestamp")
      .groupBy("uid", "hour")
      .count
      .groupBy("uid")
      .pivot("hour", hour_pivot_order)
      .sum("count")
      .na.fill(0)
      .withColumnRenamed("uid", "uid_hours")

    val fractWebHours = webLogs.withColumn("timestamp",
      to_timestamp(col("timestamp")))
      .withColumn("hour", hour(col("timestamp")))
      .drop("timestamp")
      .groupBy("uid", "hour")
      .count
      .groupBy("uid")
      .agg(
        (sum(when('hour >= 9 && 'hour < 18, 'hour).otherwise(0)) / sum('hour))
          .as("web_fraction_work_hours"),
        (sum(when('hour >= 18 && 'hour <= 23, 'hour).otherwise(0)) / sum('hour))
          .as("web_fraction_evening_hours")
      )
      .na.fill(0)
      .withColumnRenamed("uid", "uid_fract")

    val usersItems = spark.read.parquet(HDFS_DIR)

    val webDF = domainFeatures
      .join(daysWebMatrix, domainFeatures("uid1") === daysWebMatrix("uid_days"), "inner").drop("uid_days")
      .join(hoursWebMatrix, domainFeatures("uid1") === hoursWebMatrix("uid_hours"), "inner").drop("uid_hours")
      .join(fractWebHours, domainFeatures("uid1") === fractWebHours("uid_fract"), "inner").drop("uid_fract")

    val resDF = usersItems.join(webDF, usersItems("uid") === webDF("uid1"), "full").drop("uid1")

    resDF
      .write
      .format("parquet")
      .mode("overwrite")
      .save(OUT_DIR)

  }
}

