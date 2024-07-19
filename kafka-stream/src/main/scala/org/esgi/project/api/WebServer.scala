package org.esgi.project.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.StatusCodes
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.esgi.project.api.models._
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.{LikesAvg, Views}
import play.api.libs.json.Json

import scala.jdk.CollectionConverters._

object WebServer extends PlayJsonSupport {

  def routes(streams: KafkaStreams): Route = {
    concat(
      path("visits" / Segment) { period: String =>
        get {
          complete(
            List(VisitCountResponse("", 0))
          )
        }
      },
      path("latency" / "beginning") {
        get {
          complete(
            List(MeanLatencyForURLResponse("", 0))
          )
        }
      },
      path("stats" / "ten" / "best" / "score") {
        get {
          val avgStore: ReadOnlyKeyValueStore[Int, LikesAvg] = streams.store(
            StoreQueryParameters.fromNameAndType(
                StreamProcessing.likesAvgStoreName,
                QueryableStoreTypes.keyValueStore[Int, LikesAvg]()
              )
          )

          val idTitleStore: ReadOnlyKeyValueStore[String, String] = streams.store(
            StoreQueryParameters.fromNameAndType(
              StreamProcessing.idTitleStoreName,
              QueryableStoreTypes.keyValueStore[String, String]()
            )
          )

          val results = ScoreList(
            scores = avgStore.all().asScala.map { case kv =>
              val title = Option(idTitleStore.get(kv.key.toString)).getOrElse("Unknown Title")
              Score(
                id = kv.key,
                title = title,
                score = kv.value.avg
              )
            }.toList.sortBy(-_.score).take(10)
          )

          complete(StatusCodes.OK, results)
        }
      },
      path("stats" / "ten" / "worst" / "score") {
        get {
          val avgStore: ReadOnlyKeyValueStore[Int, LikesAvg] = streams.store(
            StoreQueryParameters.fromNameAndType(
              StreamProcessing.likesAvgStoreName,
              QueryableStoreTypes.keyValueStore[Int, LikesAvg]()
            )
          )

          val idTitleStore: ReadOnlyKeyValueStore[String, String] = streams.store(
            StoreQueryParameters.fromNameAndType(
              StreamProcessing.idTitleStoreName,
              QueryableStoreTypes.keyValueStore[String, String]()
            )
          )

          val results = ScoreList(
            scores = avgStore.all().asScala.map { case kv =>
              val title = Option(idTitleStore.get(kv.key.toString)).getOrElse("Unknown Title")
              Score(
                id = kv.key,
                title = title,
                score = kv.value.avg
              )
            }.toList.sortBy(+_.score).take(10)
          )

          complete(StatusCodes.OK, results)
        }
      },
      path("stats"/"ten"/"best"/"views"){
        get {
          val idTitleStore: ReadOnlyKeyValueStore[String, String] = streams.store(
            StoreQueryParameters.fromNameAndType(
              StreamProcessing.idTitleStoreName,
              QueryableStoreTypes.keyValueStore[String, String]()
            )
          )

          val countViewStore: ReadOnlyKeyValueStore[Int, Long] = streams.store(
            StoreQueryParameters.fromNameAndType(
              StreamProcessing.countViewsStoreName,
              QueryableStoreTypes.keyValueStore[Int, Long]()
            )
          )

          val results = ViewList(
            views = countViewStore.all().asScala.map { case kv =>
              val title = Option(idTitleStore.get(kv.key.toString)).getOrElse("Unknown Title")
              View(
                id = kv.key,
                title = title,
                views = kv.value
              )
            }.toList.sortBy(-_.views).take(10)
          )

          complete(StatusCodes.OK, results)
        }
      },
      path("stats"/"ten"/"worst"/"views") {
        get {
          val idTitleStore: ReadOnlyKeyValueStore[String, String] = streams.store(
            StoreQueryParameters.fromNameAndType(
              StreamProcessing.idTitleStoreName,
              QueryableStoreTypes.keyValueStore[String, String]()
            )
          )

          val countViewStore: ReadOnlyKeyValueStore[Int, Long] = streams.store(
            StoreQueryParameters.fromNameAndType(
              StreamProcessing.countViewsStoreName,
              QueryableStoreTypes.keyValueStore[Int, Long]()
            )
          )

          val results = ViewList(
            views = countViewStore.all().asScala.map { case kv =>
              val title = Option(idTitleStore.get(kv.key.toString)).getOrElse("Unknown Title")
              View(
                id = kv.key,
                title = title,
                views = kv.value
              )
            }.toList.sortBy(+_.views).take(10)
          )
          complete(StatusCodes.OK, results)
        }
      }
    )
  }
}
