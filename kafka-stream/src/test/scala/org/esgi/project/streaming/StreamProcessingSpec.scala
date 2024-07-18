package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.kstream.internals.TimeWindow
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{KeyValueStore, WindowStore}
import org.apache.kafka.streams.test.TestRecord
import org.esgi.project.streaming.StreamProcessing.viewsTopic
import org.esgi.project.streaming.models.{Likes, LikesAvg, Views}
import org.scalatest.funsuite.AnyFunSuite

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant}
import scala.jdk.CollectionConverters._

class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport {
  test("Topology Count Views"){
    val views = List(
      Views("1","movie1", "half_view"),
      Views("2","movie2", "full"),
      Views("3","movie3", "start_only"),
      Views("1","movie1", "half_view"),
      Views("3","movie3", "half_view"),
      Views("2","movie2", "full"),
      Views("2","movie2", "half_view"),
      Views("2","movie2", "full")
    )


    val topologyTestDriver = new TopologyTestDriver(
      StreamProcessing.builder.build(),
      StreamProcessing.buildProperties
    )

    val countViewsTopic = topologyTestDriver
      .createInputTopic(
        StreamProcessing.viewsTopic,
        Serdes.stringSerde.serializer(),
        toSerializer[Views]
      )

    val countViewsStore: KeyValueStore[String, Long] =
      topologyTestDriver
        .getKeyValueStore[String, Long](
          StreamProcessing.countViewsStoreName
        )

   countViewsTopic.pipeRecordList(
      views.map(view => new TestRecord(view.id, view)).asJava
    )

    assert(countViewsStore.get("1") == 2)
    assert(countViewsStore.get("2") == 4)
    assert(countViewsStore.get("3") == 2)

  }

  test("Topology Avg Likes"){
    val likes = List(
      Likes("1", 3.5),
      Likes("1", 4.5),
      Likes("2", 3)
    )


    val topologyTestDriver = new TopologyTestDriver(
      StreamProcessing.builder.build(),
      StreamProcessing.buildProperties
    )

    val avgLikeTopic = topologyTestDriver
      .createInputTopic(
        StreamProcessing.likesTopic,
        Serdes.stringSerde.serializer(),
        toSerializer[Likes]
      )

    val avgLikeStore: KeyValueStore[String, LikesAvg] =
      topologyTestDriver
        .getKeyValueStore[String, LikesAvg](
          StreamProcessing.likesAvgStoreName
        )

    avgLikeTopic.pipeRecordList(
      likes.map(like => new TestRecord(like.id, like)).asJava
    )

    assert(avgLikeStore.get("1").avg == 4)
    assert(avgLikeStore.get("2").avg == 3)

  }

  test("Topology view per category per minute"){
    val viewsNow = List(
      Views("1","movie1", "half_view"),
      Views("2","movie2", "full"),
      Views("3","movie3", "start_only"),
      Views("1","movie1", "half_view"),
    )
    val viewSevenAgo = List(
      Views("3","movie3", "half_view"),
      Views("2","movie2", "full"),
      Views("2","movie2", "half_view"),
      Views("2","movie2", "full")
    )


    val topologyTestDriver = new TopologyTestDriver(
      StreamProcessing.builder.build(),
      StreamProcessing.buildProperties
    )

    val viewTopic = topologyTestDriver
      .createInputTopic(
        StreamProcessing.viewsTopic,
        Serdes.stringSerde.serializer(),
        toSerializer[Views]
      )

    val viewPerCategoryStore: WindowStore[String, Long] =
      topologyTestDriver
        .getWindowStore[String, Long](
          StreamProcessing.viewsPerCategoryPerMinutes
        )

    val sevenMinutesAgoTimestamp = Instant.now().minus(7, ChronoUnit.MINUTES)

    viewTopic.pipeRecordList(
      viewsNow.map(view => new TestRecord(view.id, view)).asJava
    )

    val now = Instant.now().truncatedTo(ChronoUnit.MINUTES)
    val plusFive = now.plus(Duration.ofMinutes(5))

    viewTopic.pipeRecordList(
      viewSevenAgo.map(view => new TestRecord(view.id, view, plusFive)).asJava
    )

    assert(viewPerCategoryStore.fetchAll(plusFive, plusFive.plusSeconds(1)).asScala.toList.length == 2)
  }
}
