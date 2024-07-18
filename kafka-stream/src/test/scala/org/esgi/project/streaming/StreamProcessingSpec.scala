package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.test.TestRecord
import org.esgi.project.streaming.models.Views
import org.scalatest.funsuite.AnyFunSuite
import play.api.libs.json.Json

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
