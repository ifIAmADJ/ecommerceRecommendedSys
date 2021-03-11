package com.hlju.online

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object OnlineRecommender extends App {

  val STREAM_RECS = "StreamRecs"
  val PRODUCT_RECS = "ProductRecs"
  val MONGODB_RATING_COLLECTION = "Rating"

  // 调取用户最近的评分数
  val MAX_USER_RATING_NUM = 20

  // 调取候选商品列表的平分数
  val MAX_SIM_PRODUCTS_NUM = 20

  val config = Map(
    // 启动本地多线程。
    "spark.cores" -> "local[*]",
    "mongo.uri" -> "mongodb://localhost:27017/recommender",
    "mongo.db" -> "recommender",
    // 获取 kafka 的 topic
    "kafka.topic" -> "recommender"
  )

  // 创建 spark Config
  val sparkConfig: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")
  val spark: SparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()
  val sc: SparkContext = spark.sparkContext

  // 要求这段时间内能把计算完成。
  val ssc = new StreamingContext(sc, Seconds(2))

  // 导入 SparkSession 的隐式转换内容。
  import spark.implicits._

  implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

  // 提前加载相似度矩阵，为了性能考虑，可以将它广播出去。每一个 executor 保存一个副本。
  val simProductsMatrix: collection.Map[Int, Map[Int, Double]] = spark.read
    .option("uri", mongoConfig.uri)
    .option("collection", PRODUCT_RECS)
    .format("com.mongodb.spark.sql")
    .load()
    .as[ProductRecs]
    // 先将 item.rec 转换成 k-v，这样可以方便查询。
    .rdd.map {
    item => {
      (item.productId, item.recs.map(x => (x.productId, x.score)).toMap[Int, Double])
    }
  }.collectAsMap()

  // 定义广播变量
  val simProductsMatrixBc: Broadcast[collection.Map[Int, Map[Int, Double]]] = sc.broadcast(simProductsMatrix)

  // 定义 kafka 相关配置参数
  //创建到 Kafka 的连接
  val kafkaPara = Map(
    "bootstrap.servers" -> "hadoop100:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "recommender",
    "auto.offset.reset" -> "latest"
  )

  // 创建 DStream 的过程.
  val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara)
  )

  // 注意，为了得到这样的数据，这依赖另一个模块对日志进行信息处理。
  // 对 Kafka 进行处理，产生评分流，这里定义为 userId|productId|score|timestamp
  val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map {
    msg =>
      val strings: Array[String] = msg.value().split("\\|")
      (strings(0).toInt, strings(1).toInt, strings(2).toDouble, strings(3).toInt)
  }

  // 核心算法，定义处理流程
  ratingStream.foreachRDD {
    rdds =>
      rdds.foreach {
//        case (userId, productId, score, timestamp) =>
        case (userId, productId,_,_) =>
          println("rating data coming >>>>")
          // 1. 从 redis 里取出当前用户的最近评分，保存成一个数组 Array[(productId,score)]
          val userRecentlyRatings: Array[(Int, Double)] = getUserRecentlyRatings(MAX_USER_RATING_NUM, userId, ConnectionHelper.jedis)

          // 2. 从相似度矩阵中，当前商品最相似的商品列表，作为备选列表。
          // 注意，用户买过的商品就不再重复推荐了，因此这里通过传入 userId 对商品做一步 distinct.
          // 为了做这一步，需要连接到 MongoDB。
          // 最终保存成一个数组。
          val candidateProducts: Array[Int] = getTopSimProducts(MAX_SIM_PRODUCTS_NUM, productId, userId, simProductsMatrixBc.value)

          // 3. 计算每个备选商品的推荐优先级,得到当前用户的实时推荐列表，保存成一个数组 Array[(productId,score)]。
          val streamRecs: Array[(Int, Double)] = computeProductScore(candidateProducts, userRecentlyRatings, simProductsMatrixBc.value)

          // 4. 把推荐列表保存到 MongoDB。
          saveDataToMongoDB(userId, streamRecs)
      }
  }

  // 启动 streaming
  ssc.start()
  println("streaming started")
  ssc.awaitTermination()


  def getUserRecentlyRatings(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {

    import scala.collection.JavaConversions._
    // 从 redis 中用户的评分队列里获取评分数据.
    // 每个用户都会由自己的评分队列， list 键名是:uid:USERID, 值是 PRODUCTID:score
    jedis.lrange("userId:" + userId.toString, 0, num).map(
      item => {
        val attr: Array[String] = item.split("\\:")
        (attr(0).trim.toInt, attr(1).trim.toDouble)
      }
    ).toArray
  }

  def getTopSimProducts(num: Int,
                        productId: Int,
                        userId: Int,
                        simProducts: collection.Map[Int, Map[Int, Double]])(implicit mongoConfig: MongoConfig): Array[Int] = {

    // 从广播变量相似度矩阵中拿到当前商品的相似度列表
    val allSimProducts: Array[(Int, Double)] = simProducts(productId).toArray

    // 获得用户已经评分过的商品并过滤掉，排序输出
    val ratingCollection: MongoCollection = ConnectionHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
    val ratingExist: Array[Int] = ratingCollection.find(MongoDBObject("userId" -> userId))
      .toArray
      .map(_.get("productId").toString.toInt)

    allSimProducts.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num)
      .map(_._1)

  }


  // 计算每个备选商品的最近 num 评分。
  // 最终的返回结果是一个 Array[(Int,Double)] 类型。第一个 Int 指代商品编号，第二个 Double 则是商品的评分。
  def computeProductScore(candidateProducts: Array[Int],
                          userRecentlyRatings: Array[(Int, Double)],
                          simProducts: collection.Map[Int, Map[Int, Double]]): Array[(Int, Double)] = {

    // 定义一个可变数组 ArrayBuffer, 保存每一个备选商品的基础得分，(productId,score)
    val scores: ArrayBuffer[(Int, Double)] = ArrayBuffer[(Int, Double)]()
    // 定义两个 Map，分别记录对于每一个商品的奖励项和惩罚项。
    // 它记录的的是 (productId,increCount)
    val increMap: mutable.HashMap[Int, Int] = mutable.HashMap[Int, Int]()

    // 它记录的是 (productId,decreCount)
    val decreMap: mutable.HashMap[Int, Int] = mutable.HashMap[Int, Int]()

    for {
      candidateProduct <- candidateProducts
      userRecentlyRating <- userRecentlyRatings
    } {

      // 从相似度矩阵中获取当前备选商品和最近已评分商品之间的相似度。
      // 查找的过程被封装成了函数。
      val simScore: Double = getProductsSimScore(candidateProduct, userRecentlyRating._1, simProducts)

      // 规定相似度至少要大于 0.4
      if (simScore > 0.4) {
        // 按照公式进行加权计算

        scores += ((candidateProduct, simScore * userRecentlyRating._2))

        // 如果是 三星以上好评，则标记为推荐，否则标记为不推荐。
        if (userRecentlyRating._2 > 3) {
          increMap(candidateProduct) = increMap.getOrElse(candidateProduct, 0) + 1
        } else {
          decreMap(candidateProduct) = decreMap.getOrElse(candidateProduct, 0) + 1
        }
      }
    }

    // 这里对应着备选商品的推荐优先级计算公式。
    // 返回推荐列表，按照得分排序。
    scores.groupBy(_._1).map {
      case (productId, scoreList) =>
        (productId, (scoreList.map(_._2).sum / scoreList.length) + lg(increMap.getOrElse(productId, 1)) - lg(decreMap.getOrElse(productId, 1)))
    }.toArray.sortWith(_._2 > _._2)

  }

  // 传入两个商品的 productId，和相似度矩阵，然后返回相似度。
  // 这个函数只是简单地查找
  def getProductsSimScore(product1: Int, product2: Int,
                          simProducts: collection.Map[Int, Map[Int, Double]]): Double = {
    simProducts.get(product1) match {
      case Some(sims) =>
        sims.get(product2) match {
          case Some(score) => score
          case None => 0.0d
        }
      case None => 0.0d
    }
  }

  // 利用换底公式实现 lg 函数
  def lg(m : Int):Double = {
    val N = 10
    math.log(m) / math.log(N)
  }

  def saveDataToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    val streamRecsCollection: MongoCollection = ConnectionHelper.mongoClient(mongoConfig.db)(STREAM_RECS)
    // 按照 userId 查询并更新
    streamRecsCollection.findAndRemove(MongoDBObject("userId"-> userId))
    streamRecsCollection.insert(MongoDBObject("userId"-> userId,"recs"->streamRecs.map(x=>MongoDBObject("productId"->x._1,"score"->x._2))))
  }

}


// 创建一个可序列化单例对象，方便在网络环境中传输
object ConnectionHelper extends Serializable {

  // 默认是本地的 localhost.
  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))

}

case class MongoConfig(uri: String, db: String)

// 定义标准的推荐对象
case class Recommendation(productId: Int, score: Double)

// 定义用户的推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])

// 定义商品相似度的列表
case class ProductRecs(productId: Int, recs: Seq[Recommendation])