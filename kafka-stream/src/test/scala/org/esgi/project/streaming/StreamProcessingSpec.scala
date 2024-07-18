package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.TestRecord
import org.esgi.project.streaming.models.Views
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport {
  test("Topology should compute a correct best of 3 movies by views"){
    val views = List(
      Views("1","movie1", "half_view"),
      Views("2","movie2", "full"),
      Views("3","movie3", "start_only"),
      Views("1","movie4", "half_view"),
      Views("3","movie5", "half_view"),
      Views("2","movie6", "full"),
      Views("2","movie7", "half_view"),
      Views("2","movie8", "full")
    )


    val topologyTestDriver = new TopologyTestDriver(
      StreamProcessing.builder.build(),
      StreamProcessing.buildProperties
    )

    val bestOfViewsTopic = topologyTestDriver
      .createInputTopic(
        StreamProcessing.viewsTopic,
        Serdes.stringSerde.serializer(),
        PlayJsonSupport.
      )

    val bestOfViewsStore: KeyValueStore[String, Long] =
      topologyTestDriver
        .getKeyValueStore[String, Long](
          StreamProcessing.topViewsStoreName
        )

    bestOfViewsTopic.pipeRecordList(
      views.map(view => new TestRecord(view.title, view.title)).asJava
    )

    assert(bestOfViewsStore.get("movie1") == 2)
    assert(bestOfViewsStore.get("movie2") == 4)
    assert(bestOfViewsStore.get("movie3") == 2)

  }
  test("Topology should compute a correct word count") {
    // Given
    val messages = List(
      "hello world",
      "hello moon",
      "foobar",
      "42"
    )

    val topologyTestDriver = new TopologyTestDriver(
      StreamProcessing.builder.build(),
      StreamProcessing.buildProperties
    )

    val wordTopic = topologyTestDriver
      .createInputTopic(
        StreamProcessing.wordTopic,
        Serdes.stringSerde.serializer(),
        Serdes.stringSerde.serializer()
      )

    val wordCountStore: KeyValueStore[String, Long] =
      topologyTestDriver
        .getKeyValueStore[String, Long](
          StreamProcessing.wordCountStoreName
        )

    // When
    wordTopic.pipeRecordList(
      messages.map(message => new TestRecord(message, message)).asJava
    )

    // Then
    assert(wordCountStore.get("hello") == 2)
    assert(wordCountStore.get("world") == 1)
    assert(wordCountStore.get("moon") == 1)
    assert(wordCountStore.get("foobar") == 1)
    assert(wordCountStore.get("42") == 1)
  }
}
