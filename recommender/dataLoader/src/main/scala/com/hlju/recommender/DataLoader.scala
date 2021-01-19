package com.hlju.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 加载 CSV 对象，然后上传至 MongoDB 数据库当中
  * product 数据集包含了: (商品 ID, 商品名称, 商品分类 ID, 亚马逊 ID, 图片 URL, 商品分类, UGC 标签)
  * ratings 数据集包含了: (用户 ID, 商品 ID, 评分, 时间戳)
  */
object DataLoader {

  // 定义数据源的文件路径。
  val PRODUCT_DATA_PATH = "recommender/dataLoader/src/main/resources/products.csv"
  val RATING_DATA_PATH = "recommender/dataLoader/src/main/resources/ratings.csv"

  // Mongodb 对应数据源的集合 (表) 名
  val MONGODB_PRODUCT_COLLECTION = "product"
  val MONGODB_RATINGS_COLLECTION = "ratings"

  def main(args: Array[String]): Unit = {

    val config = Map(
      // 启动本地多线程。
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个 spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 导入 SparkSession 的隐式转换内容。
    import spark.implicits._


    //加载数据,将它加载成一个弹性数据集
    val productRDD: RDD[String] = spark.sparkContext.textFile(PRODUCT_DATA_PATH)

    // 返回一个数据帧
    val productDF: DataFrame = productRDD.map(
      // 解析 product csv 文件
      item => {
        val attr: Array[String] = item.split("\\^")
        Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
      }
    ).toDF()

    val ratingRDD: RDD[String] = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF: DataFrame = ratingRDD.map(
      item => {
        val attr: Array[String] = item.split(",")
        Rating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
      }
    ).toDF()

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    storeDataInMongoDB(productDF,ratingDF)

    spark.stop()
  }

  def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig : MongoConfig): Unit = {

    // 新建一个 mongoDB 连接
    val mongoClient  = MongoClient(MongoClientURI(mongoConfig.uri))

    // 定义要操作的 mongoDB 表
    val productCollection: MongoCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection: MongoCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)

    // 如果表已经存在，那么先删除
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    // 将当前数据存入对应的表当中。直接从数据帧调用 write 方法也可以
    productDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATINGS_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 对标创建一个索引
    productCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("userId" -> 1))

    mongoClient.close()

  }
}

case class Product(productId: Int, name: String, imageURL: String, categories: String, tags: String)
case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)
case class MongoConfig(uri: String, db: String)