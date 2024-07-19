package org.esgi.project.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.StatusCodes
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore}
import org.esgi.project.api.models._
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.{LikesAvg, ViewPerCategory, Views}
import play.api.libs.json.Json

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, OffsetDateTime, ZoneId}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object WebServer extends PlayJsonSupport {



  def routes(streams: KafkaStreams): Route = {
    val idTitleStore: ReadOnlyKeyValueStore[String, String] = streams.store(
      StoreQueryParameters.fromNameAndType(
        StreamProcessing.idTitleStoreName,
        QueryableStoreTypes.keyValueStore[String, String]()
      )
    )

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
      path("movies" / Segment) { movieIdStr =>
        get {
          val movieId = movieIdStr.toInt

          val viewCountStore: ReadOnlyKeyValueStore[Int, Long] = streams.store(
            StoreQueryParameters.fromNameAndType(
              StreamProcessing.countViewsStoreName,
              QueryableStoreTypes.keyValueStore[Int, Long]()
            )
          )

          val idTitleStore: ReadOnlyKeyValueStore[String, String] = streams.store(
            StoreQueryParameters.fromNameAndType(
              StreamProcessing.idTitleStoreName,
              QueryableStoreTypes.keyValueStore[String, String]()
            )
          )

          val viewCategoryTotalStore: ReadOnlyKeyValueStore[Int, ViewPerCategory] = streams.store(
            StoreQueryParameters.fromNameAndType(
              StreamProcessing.viewsPerCategoryTotalStoreName,
              QueryableStoreTypes.keyValueStore[Int, ViewPerCategory]()
            )
          )

          val viewPerCategoryStore: ReadOnlyWindowStore[Int, ViewPerCategory] = streams.store(
            StoreQueryParameters.fromNameAndType(
              StreamProcessing.viewsPerCategoryPerMinutes,
              QueryableStoreTypes.windowStore[Int, ViewPerCategory]
            )
          )

          val viewCount = Option(viewCountStore.get(movieId)).getOrElse(-1L)
          val title = Option(idTitleStore.get(movieId.toString)).getOrElse("Unknown Title")
          val pastCategory = viewCategoryTotalStore.get(movieId)

          val currentTime = Instant.now().truncatedTo(ChronoUnit.MINUTES)
          val fiveMinutesAgo = currentTime.minus(Duration.ofMinutes(5))

          val windowsList = ListBuffer[ViewPerCategory]()
          val window = viewPerCategoryStore.fetch(movieId, fiveMinutesAgo, currentTime).next()


          val result = Movie(
            id = movieId,
            title = title,
            total_view_count = viewCount,
            past = pastCategory,
            last_five_minutes = window.value
          )

          complete(StatusCodes.OK, result)
        }
      }
    )
  }
}
