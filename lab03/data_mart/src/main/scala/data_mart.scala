import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

import scala.util.Try
import java.net.URL
import java.net.URLDecoder.decode


object data_mart extends App {

  val spark = SparkSession
    .builder()
    .appName("lab03")
    .master("local[*]")
    .getOrCreate()

  val ES_USERNAME: String = "vladimir.cherny"
  val ES_PASSWORD: String = "ZyqQ3Mdj"
  val ELASTIC_HOST = "10.0.0.5:9200"

  val esOptions =
    Map(
      "es.nodes" -> ELASTIC_HOST,
      "es.batch.write.refresh" -> "false",
      "es.net.http.auth.user" -> ES_USERNAME,
      "es.net.http.auth.pass" -> ES_PASSWORD,
      "es.nodes.wan.only" -> "true"
    )

  var shops = spark
    .read
    .format("org.elasticsearch.spark.sql")
    .options(esOptions)
    .load("visits")
    .toDF

  val input = spark.read
    .option("header", "true")
    .json("/labs/laba03/weblogs.json")
    .toDF
    .select(col("uid"), explode(col("visits")))
    .select(col("uid"), col("col.*"))
    .toDF

  val webLogs = input.na.drop(List("uid"))

  val CASSANDRA_HOST = "10.0.0.5"
  val CASSANDRA_PORT = "9042"

  spark.conf.set("spark.cassandra.connection.host", CASSANDRA_HOST)
  spark.conf.set("spark.cassandra.connection.port", CASSANDRA_PORT)
  spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
  spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

  val cOpts = Map("table" -> "clients", "keyspace" -> "labdata")

  val clients = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(cOpts)
    .load
    .toDF

  val POSTGRE_USER = "vladimir_cherny"
  val POSTGRE_URL = s"jdbc:postgresql://10.0.0.5:5432/labdata?user=$POSTGRE_USER&password=$ES_PASSWORD"

  val pgOptions = Map(
    "url" -> POSTGRE_URL,
    "dbtable" -> "domain_cats",
    "user" -> POSTGRE_USER,
    "password" -> ES_PASSWORD,
    "driver" -> "org.postgresql.Driver"
  )

  val webCats = spark
    .read
    .format("jdbc")
    .options(pgOptions)
    .load
    .toDF

  val new_col = when(clients("age").between(18, 24), "18-24")
    .when(clients("age").between(25, 34), "25-34")
    .when(clients("age").between(35, 44), "35-44")
    .when(clients("age").between(45, 54), "45-54")
    .when(clients("age") >= 55, ">=55")
  val cat_clients_age = clients.withColumn("age_cat", new_col)
  val cat_clients = cat_clients_age.drop("age")

  val decode_url = udf { (url: String) => Try(new URL(decode(url, "UTF-8")).getHost).toOption }

  val filtered_logs = webLogs
    .filter(col("url").startsWith("http"))
    .withColumn("url", decode_url(col("url")))
    .withColumn("url", regexp_replace(col("category"), "^www.", ""))
    .dropDuplicates

  val websitesWithCats = filtered_logs
    .join(webCats, filtered_logs("url") === webCats("domain"))
    .groupBy("uid", "category").count
    .withColumn("category", concat(lit("web_"), col("category")))

  val usersToCats = websitesWithCats
    .groupBy("uid")
    .pivot("category")
    .sum("count")
    .na.fill(0)

  val shopsWithCats = shops
    .select(col("uid"), col("category"))
    .withColumn("category", lower(col("category")))
    .withColumn("category", regexp_replace(col("category"), "[ -]", "_"))
    .groupBy("uid", "category").count
    .withColumn("category", concat(lit("shop_"), col("category")))

  val usersToShops = shopsWithCats
    .groupBy("uid")
    .pivot("category")
    .sum("count")
    .na.fill(0)

  val right = usersToShops.withColumnRenamed("uid", "right_uid")

  val tmp = cat_clients
    .join(right, right("right_uid") === cat_clients("uid"), "left")
    .drop("right_uid")
    .na.fill(0)

  val right1 = usersToCats.withColumnRenamed("uid", "right_uid")

  val res = tmp
    .join(right1, right1("right_uid") === tmp("uid"), "left")
    .drop("right_uid")
    .na.fill(0)

  val writeOptions = Map(
    "url" -> "jdbc:postgresql://10.0.0.5:5432/vladimir_cherny",
    "dbtable" -> "clients",
    "user" -> POSTGRE_USER,
    "password" -> ES_PASSWORD,
    "driver" -> "org.postgresql.Driver")

  res
    .write
    .format("jdbc")
    .options(writeOptions)
    .mode("overwrite")
    .save

}