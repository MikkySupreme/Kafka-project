package org.esgi.project.streaming

import scala.util.Random
import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KTable, Materialized}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.esgi.project.streaming.models.{Likes, LikesAvg, ViewPerCategory, Views}

import java.time.Duration
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  implicit val likesSerde: Serde[Likes] = toSerde[Likes]
  implicit val viewsSerde: Serde[Views] = toSerde[Views]
  implicit val likesAvgSerde: Serde[LikesAvg] = toSerde[LikesAvg]

  val applicationName = s"some-application-name"

  private val props: Properties = buildProperties

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  val viewsTopic = "views"
  val likesTopic = "likes"

  val idTitleStoreName = "idTitle"
  val countViewsStoreName = "countViews"
  val countViewsByTypeOfView = "countViewsByTypeOfView"
  val likesAvgStoreName = "likes-average"
  val viewsPerCategoryPerMinutes = "viewsPerCategoryPerminutes"

  val views = builder.stream[String, Views](viewsTopic)
  val likes = builder.stream[String, Likes](likesTopic)

  val titles: KTable[String, String] = views
    .groupByKey.aggregate("")((_, view, _) => view.title)(Materialized.as(idTitleStoreName))


  val viewsGroupById : KGroupedStream[Int, Views] = views
    .groupBy(
    (_, view) => view.id
  )

  val viewsGroupByCategory : KGroupedStream[String, Views] = views
    .groupBy(
      (_, view) => view.view_category
    )

  val viewsCount: KTable[Int, Long] = viewsGroupById
    .count()(Materialized.as(countViewsStoreName))


  val likesAvg: KTable[Int, LikesAvg] = likes
    .groupBy((_, like) => like.id)
    .aggregate(
      initializer = LikesAvg.empty
    )(
      aggregator = (_, like, agg) => {
        agg.increment(like.score)
      }
    )(Materialized.as(likesAvgStoreName))

  val viewPerCategoryPerMinute: KTable[Windowed[Int], ViewPerCategory] = views.groupBy(
      (_, view) => view.id
    ).windowedBy(
      TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).advanceBy(Duration.ofSeconds(1))
    )
    .aggregate(
      initializer = ViewPerCategory.empty
    )(
      aggregator = (_, view, agg) => {
        agg.increment(view.view_category)
      }
    )(Materialized.as(viewsPerCategoryPerMinutes))

  val viewsPerCategoryTotalStoreName = "viewsPerCategoryTotal"
  val viewPerCategoryTotal: KTable[Int, ViewPerCategory] = views.groupBy(
      (_, view) => view.id
  ).aggregate(
      initializer = ViewPerCategory.empty
    )(
      aggregator = (_, view, agg) => {
        agg.increment(view.view_category)
      }
    )(Materialized.as(viewsPerCategoryTotalStoreName))



  def randomizeString(input: String): String = {
    val random = new Random()
    val chars = input.toCharArray
    random.shuffle(chars.toSeq).mkString
  }

  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = {
        streams.close()
      }
    }))
    streams
  }

  // auto loader from properties file in project
  def buildProperties: Properties = {
    val appName = randomizeString(applicationName)
    val properties = new Properties()
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, appName)
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, appName)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    properties
  }
}
