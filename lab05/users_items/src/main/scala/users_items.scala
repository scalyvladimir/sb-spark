import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object users_items{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Lab05")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val USER = "vladimir.cherny"
    val HDFS_DIR = s"/user/$USER/visits"

    val conf_update = spark.conf.get("spark.users_items.update")
    val conf_input_dir = spark.conf.get("spark.users_items.input_dir")
    val conf_output_dir = spark.conf.get("spark.users_items.output_dir")

    def union_cols(myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map( x => x
        match {
          case x if myCols.contains(x) => col(x)
          case _ => lit(0).as(x)
        }
      )
    }

    val data = spark.read
      .option("header", true)
      .json(HDFS_DIR + "/*/*/*.json")
      .toDF

    val MAX_DATE = data.select(date_format(max(('timestamp / 1000).cast("timestamp")), "yyyyMMdd"))
                        .collect()(0)(0).toString

    if (conf_update.equals("0"))
    {
      val dataTransformed = data
        .select('uid, 'event_type, 'item_id)
        .filter('uid.isNotNull)
        .withColumn("item",
          lower(concat(
            concat('event_type, lit("_")),  regexp_replace('item_id, "[ -]", "_"))))
        .drop("event_type", "item_id")
        .groupBy("uid", "item").count

      val dataMatrix = dataTransformed
        .groupBy("uid")
        .pivot("item")
        .sum("count")
        .na.fill(0)

      dataMatrix
        .write
        .format("parquet")
        .mode("overwrite")
        .save(conf_output_dir + s"/${MAX_DATE}")
    }

    if (conf_update.equals("1"))
    {
      val OLD_MAX_DATE = MAX_DATE

      val oldMatrix = spark.read.parquet(s"${conf_output_dir}/${OLD_MAX_DATE}/*")

      val newData = spark.read
        .option("header", true)
        .json(conf_input_dir + "/*/*/*.json")
        .toDF

      val NEW_MAX_DATE = newData.select(date_format(max(('timestamp / 1000).cast("timestamp")), "yyyyMMdd")).collect()(0)(0).toString

      val newMatrix = newData
        .select('uid, 'event_type, 'item_id)
        .filter('uid.isNotNull)
        .withColumn("item",
          lower(concat(
            concat('event_type, lit("_")),  regexp_replace('item_id, "[ -]", "_"))))
        .drop("event_type", "item_id")
        .groupBy("uid", "item")
        .count
        .groupBy("uid")
        .pivot("item")
        .sum("count")
        .na.fill(0)

      val oldCols = oldMatrix.columns.toSet
      val newCols = newMatrix.columns.toSet
      val total = oldCols ++ newCols

      val resMatrix = oldMatrix.select(union_cols(oldCols, total):_*).union(newMatrix.select(union_cols(newCols, total):_*))

      resMatrix
        .write
        .format("parquet")
        .mode("overwrite")
        .save(conf_output_dir + s"/${NEW_MAX_DATE}")
    }

  }
}

