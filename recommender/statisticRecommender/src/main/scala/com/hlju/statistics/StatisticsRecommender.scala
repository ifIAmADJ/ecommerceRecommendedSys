package com.hlju.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 商品的热门信息取决于评分信息，而和商品本身的关系不大。
  */
object StatisticsRecommender {

  // mongoDB 当中的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  // 历史的热门商品统计
  val RATE_MORE_PRODUCTS = "RateMoreProducts"

  // 最近的热门商品统计
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"

  // 每一个商品的平均评分统计
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {
    val config = Map(
      // 启动本地多线程。
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个 spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("statisticsRecommender")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 导入 SparkSession 的隐式转换内容。
    import spark.implicits._

    // 导入 SparkSession 的隐式转换内容。
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据
    val ratingDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    // 创建一张叫 ratings0 的临时表(视图)
    ratingDF.createOrReplaceTempView("ratings0")

    //TODO: 用 spark sql 去做不同的统计推荐
    // 1.历史热门商品，按照评分个数统计
    val rateMoreProductDF: DataFrame = spark.sql(
      """
        | select productId,count(productId) as count from ratings0
        | group by productId
        | order by count desc
      """.stripMargin
    )
    storeDFInMongoDB(rateMoreProductDF, RATE_MORE_PRODUCTS)

    // 2.近期热门商品，把时间戳转换成 yyyyMM 格式进行评分个数统计
    // 创建一个日期的格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册 UDF，将 timestamp 转化为年月格式 yyyyMM
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    // 原始 ratings 数据转换成目标 productId, score, yearMonth
    val ratingOfYearMonthDF: DataFrame = spark.sql(
      """
        | select productId,score,changeDate(timestamp) as yearMonth
        | from ratings0
      """.stripMargin
    )
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyProductsDF: DataFrame = spark.sql(
      """
        | select productId,count(productId) as count, yearMonth
        | from ratingOfMonth
        | group by yearMonth,productId
        | order by yearMonth desc, count desc
      """.stripMargin
    )
    //将 df 保存到 MongoDB 当中
    storeDFInMongoDB(rateMoreRecentlyProductsDF, RATE_MORE_RECENTLY_PRODUCTS)


    // 3.优质商品统计，商品的平均评分
    val averageProductsDF: DataFrame = spark.sql(
      """
        | select productId,avg(score) as avg from ratings0
        | group by productId
        | order by avg desc
      """.stripMargin
    )
    storeDFInMongoDB(averageProductsDF,AVERAGE_PRODUCTS)

    spark.stop()

  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)
case class MongoConfig(uri: String, db: String)
