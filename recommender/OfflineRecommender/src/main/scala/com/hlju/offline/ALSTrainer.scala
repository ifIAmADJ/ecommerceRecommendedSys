package com.hlju.offline

import com.hlju.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {

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
    val ratingRDD: RDD[Rating] = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd.map(
      rating => Rating(rating.userId, rating.productId, rating.score)
    ).cache()

    // 划分训练集和测试集。
    // 数据切分成训练集和测试集, 8-2 开
    val splits: Array[RDD[Rating]] = ratingRDD.randomSplit(Array(0.8, 0.2))

    val trainingRDD = splits(0)
    val testingRDD = splits(1)

    // 核心实现：输出最优参数
    adjustALSParams(trainingRDD, testingRDD)

    spark.stop()
  }

  def getRMSE(model: MatrixFactorizationModel, testingRDD: RDD[Rating]): Double = {
    // 构建 userProducts, 得到预测评分矩阵
    val userProducts: RDD[(Int, Int)] = testingRDD.map(item => (item.user, item.product))
    val predictRating: RDD[Rating] = model.predict(userProducts)

    // 按照公式计算 rmse，按照 (item.user,item.product) 连接 key
    val observed: RDD[((Int, Int), Double)] = testingRDD.map(
      item => ((item.user, item.product), item.rating)
    )

    val predict: RDD[((Int, Int), Double)] = predictRating.map(
      item => ((item.user, item.product), item.rating)

    )

    Math.sqrt(observed.join(predict).map {
      case ((userId, predictId), (actual, pre)) =>
        val err: Double = actual - pre
        err * err
    }.mean())

  }

  def adjustALSParams(trainingRDD: RDD[Rating], testingRDD: RDD[Rating]): Unit = {
    // 遍历数组中定义的参数取值
    val result: Array[(Int, Double, Double)] = for (rank <- Array(20, 30, 50, 100); lambda <- Array(1, 0.1, 0.01)) yield {
      val model: MatrixFactorizationModel = ALS.train(trainingRDD, rank, 10, lambda)
      val rmse: Double = getRMSE(model, testingRDD)
      (rank, lambda, rmse)
    }

    // 输出最优的 rank 和 lambda 参数，rmse = 1.28d
    // (100,0.01,1.2858374868559026)
    println(result.minBy(_._3))
  }
}
