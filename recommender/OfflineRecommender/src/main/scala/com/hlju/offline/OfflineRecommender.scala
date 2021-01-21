package com.hlju.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

object OfflineRecommender {
  // mongoDB 当中的表名
  val MONGODB_RATING_COLLECTION = "Rating"
  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"
  val USER_MAX_RECOMMENDATION = 20


  def main(args: Array[String]): Unit = {
    val config = Map(
      // 启动本地多线程。
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个 spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 导入 SparkSession 的隐式转换内容。
    import spark.implicits._

    // 导入 SparkSession 的隐式转换内容。
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 读取 rating 数据类型
    val ratingRDD: RDD[(Int, Int, Double)] = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd.map(
      rating => (rating.userId, rating.productId, rating.score)
    ).cache()

    // 提取出参与评分的用户和
    val userRdd: RDD[Int] = ratingRDD.map(_._1).distinct()
    val productRdd: RDD[Int] = ratingRDD.map(_._2).distinct()

    //核心计算过程
    //1. 训练隐语义模型，需要和 spark-mllib 的 Rating 类型相兼容，这一步要做转化
    val trainData: RDD[Rating] = ratingRDD.map(x => Rating(x._1, x._2, x._3))

    /*

      rank: 隐特征的维度 k
      iterations: 迭代次数
      lambda: 正则化系数，0.01

      这里只给定初始值。后续通过计算方均误差计算模型。
      rank = 100
      lambda = 0.01

     */

    val (rank, iterations, lambda) = (100, 10, 0.01)
    val model: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, lambda)

    // 2. 获得预测评分矩阵，进而得到用户的推荐列表
    val userProducts: RDD[(Int, Int)] = userRdd.cartesian(productRdd)
    val preRating: RDD[Rating] = model.predict(userProducts)


    /*
      从预测评分矩阵中提取到用户推荐列表，首先做一个预筛选，再做转换。
      转换步骤是: 取出 (productId,rating)，按照第二个元素比较大小 (比较评分)
      取出前20条记录之后，转换成目标的 Recommendation 类型。

      最终，这些记录由 RDD 转换为 DataFrame 写入到 MongoDB 数据库内
    */

    val userRecs: DataFrame = preRating.filter(_.rating > 0)
      .map {
        rating => (rating.user, (rating.product, rating.rating))
      }.groupByKey()
      .map {
        // 利用偏函数的模式匹配写法直接提取出了userId和(rating.product,rating.rating)
        case (userId, recs) =>
          UserRecs(userId, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //3. 利用商品的特征向量，顺便计算商品的相似度列表，这里转换成了 jblas 形式的矩阵.
    val productFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map {
      case (productId, features) => (productId, new DoubleMatrix(features))
    }

    // 计算相似度，这里需要两两配对商品(和自己做笛卡尔积)，计算余弦相似度。
    // 还有一个问题，对角线位置的 "自己" 和 "自己" 配对相似度一定是最高的，要过滤掉这种情况。
    val productRecs: DataFrame =
    productFeatures.cartesian(productFeatures).filter {
      case (a, b) => a._1 != b._1
    }.map {
      case (a, b) =>
        // 返回(商品1, (商品2, 匹配度))。simScore 相似度的结果在 0 ~ 1 之间，越高说明越相似。
        val simScore: Double = consinSim(a._2, b._2)
        (a._1, (b._1, simScore))
    }.filter(_._2._2 > 0.4)
      .groupByKey()
      .map {
        // 利用偏函数的模式匹配写法直接提取出了userId和(rating.product,rating.rating)
        case (productId, recs) =>
          ProductRecs(productId, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  // 计算余弦相似度的计算公式
  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix): Double = {
    // 模长是 l2 范数
    product1.dot(product2) / (product1.norm2() * product2.norm2())
  }

}

case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

// 定义标准的推荐对象
case class Recommendation(productId: Int, score: Double)

// 定义用户的推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])

// 定义商品相似度的列表
case class ProductRecs(productId: Int, recs: Seq[Recommendation])