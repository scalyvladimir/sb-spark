import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object agg{

  def main(args: Array[String]): Unit = {

    val USER = "vladimir.cherny"
    val IN_TOPIC = "vladimir_cherny"
    val OUT_TOPIC = "vladimir_cherny_lab04b_out"

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

//    def killAll() = {
//      SparkSession
//        .active
//        .streams
//        .active
//        .foreach { x =>
//          val desc = x.lastProgress.sources.head.description
//          x.stop
//          println(s"Stopped ${desc}")
//        }
//    }

    val sdf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", IN_TOPIC)
      .option("maxOffsetsPerTrigger", 200)
      .load

    // val value_cols = List(
    //     "event_type",
    //     "category",
    //     "item_id",
    //     "item_price",
    //     "uid",
    //     "timestamp"
    // )

    // def parse_json_value (sdf: DataFrame, schema: List[String]): DataFrame = {

    //     var parsedSdf = sdf.select('value.cast("string")).as[String]
    //             .withColumn("value", regexp_replace('value, "[{]", ""))
    //             .withColumn("tokens", split('value, ","))
    //             .select('tokens)

    //     val regexp_str = "\"|}|:| |"
    //     val ind_schema = schema.zipWithIndex.toMap

    //     for (col_name <- schema)
    //     {
    //         parsedSdf = parsedSdf
    //                         .withColumn(s"${col_name}", 'tokens(ind_schema(s"${col_name}")))
    //                         .withColumn(s"${col_name}", regexp_replace(col(s"${col_name}"), regexp_str, ""))
    //                         .withColumn(s"${col_name}", regexp_replace(col(s"${col_name}"), s"${col_name}", ""))
    //     }
    //     parsedSdf
    //         .drop("tokens")

    // }

    val schema: StructType = StructType(
      StructField("event_type", StringType, true) ::
        StructField("category", StringType, true) ::
        StructField("item_id", StringType, true) ::
        StructField("item_price", LongType, true) ::
        StructField("uid", StringType, true) ::
        StructField("timestamp", LongType, true) ::
        Nil
    )

    def parse_json_value (sdf: DataFrame, schema: StructType): DataFrame = {

      var parsedSdf = sdf.select(from_json('value.cast("string"), schema).alias("value"))
        .select(col("value.*"))
      parsedSdf

    }

    val parsedSdf = parse_json_value(sdf, schema)
      .withColumn("date", to_date(from_unixtime('timestamp / 1000)))
      .withColumn("date", from_unixtime(unix_timestamp('date, "yyyy-MM-dd"), "yyyyMMdd"))

    val res_df = parsedSdf
      .groupBy(window(to_timestamp('timestamp), "1 hours"))
      .agg(
        sum(when(col("event_type") === "buy", col("item_price")).otherwise(0)).alias("revenue"),
        sum(when(col("uid").isNotNull, 1).otherwise(0)).alias("visitors"),
        sum(when(col("event_type") === "buy", 1).otherwise(0)).alias("purchases")
      )
      .select(col("window.*"), 'revenue, 'visitors, 'purchases)
      .withColumnRenamed("start", "start_ts")
      .withColumnRenamed("end", "end_ts")
      .withColumn("start_ts", unix_timestamp(col("start_ts")))
      .withColumn("end_ts", unix_timestamp(col("end_ts")))
      .withColumn("aov", 'revenue / 'purchases)
      .na.fill(0, Array("aov"))

    // val tmpDFs = Seq(revenue, visitors, purchases)


    // def join_mult_dfs (DFs: Seq[DataFrame]): DataFrame = {

    //     var res_df: DataFrame = spark.emptyDataFrame
    //     var tmp_df: DataFrame = spark.emptyDataFrame

    //     for (ind <- 0 to DFs.length - 1)
    //     {
    //         if (ind == 0)
    //             res_df = DFs(ind)
    //         else
    //         {
    //             tmp_df = DFs(ind).withColumnRenamed("start_ts", "s").withColumnRenamed("end_ts", "e")
    //             res_df = res_df.join(
    //                 tmp_df,
    //                 (res_df("start_ts") === tmp_df("s") && res_df("end_ts") === tmp_df("e")),
    //                 "inner"
    //             )
    //             .drop("s")
    //             .drop("e")
    //         }
    //     }
    //     res_df

    // }


    // val res = join_mult_dfs(tmpDFs).withColumn("aov", 'revenue / 'purchases)

    def createConsoleSink(df: DataFrame) = {
      df
        .writeStream
        .outputMode("update")
        .format("kafka")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .option("checkpointLocation", s"/tmp/$USER/chk")
        .option("kafka.bootstrap.servers", "spark-master-1:6667")
        .option("subscribe", OUT_TOPIC)
        .option("maxOffsetsPerTrigger", 200)
    }

    spark.stop
  }
}

