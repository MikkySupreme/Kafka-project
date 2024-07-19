package org.esgi.project.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.StatusCodes
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.esgi.project.api.models.TenBestViews.format
import org.esgi.project.api.models._
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.LikesAvg
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
          val store: ReadOnlyKeyValueStore[Int, LikesAvg] = streams.store(
            StoreQueryParameters.fromNameAndType(
                StreamProcessing.likesAvgStoreName,
                QueryableStoreTypes.keyValueStore[Int, LikesAvg]()
              )
          )

          val results = TenBestScore(
            aggregation = store.all().asScala.map(kv => Score(
              id = kv.key,
              title = "Title Placeholder",
              score = kv.value.avg
            )).toList
          )

          complete(StatusCodes.OK, results)
        }
      },
      path("stats"/"ten"/"best"/"views"){
        get{
          val store: ReadOnlyKeyValueStore[Int, Long] = streams.store(
            StoreQueryParameters.fromNameAndType(
              StreamProcessing.countViewsStoreName,
              QueryableStoreTypes.keyValueStore[Int, Long]()
            )
          )
          val results = store.all().asScala.toList
            .map(kv => Viewed(
              id = kv.key,
              title = "Title Placeholder",
              views = kv.value
            ))
            .sortBy(-_.views)
            .take(10)

          complete(StatusCodes.OK, Json.toJson(TenBestViews(results)))
        }
      }
    )
  }
}
