package com.hlju.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 商品的热门信息取决于评分信息，而和商品本身的关系不大。
  */
object StatisticsRecommender {

  // mongoDB 当中的表名
  val MONGODB_RATINGS_COLLECTION = "ratings"

  // 历史的热门商品统计
  val RATE_MORE_PRODUCTS = "rateMoreProducts"

  // 最近的热门商品统计
  val RATE_MORE_RECENTLY_PRODUCTS = "rateMoreRecentlyProducts"

  // 每一个商品的平均评分统计
  val AVERAGE_PRODUCTS = "averageProducts"

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
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    // 加载数据
    val ratingDF: DataFrame = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATINGS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    // 创建一张叫 ratings 的临时表(视图)
    ratingDF.createOrReplaceTempView("ratings")

    //TODO: 用 spark sql 去做不同的统计推荐
    // 1.历史热门商品，按照评分个数统计

    // 2.近期热门商品，把时间戳转换成 yyyyMM 格式进行评分个数统计

    // 3.优质商品统计，商品的平均评分
    spark.stop()

  }

}

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)
case class MongoConfig(uri: String, db: String)
