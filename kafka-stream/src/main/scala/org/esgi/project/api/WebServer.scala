package org.esgi.project.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.StatusCodes

import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.esgi.project.api.models.{MeanLatencyForURLResponse, VisitCountResponse}
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.LikesAvg

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
            StoreQueryParameters
              .fromNameAndType(
                StreamProcessing.likesAvgStoreName,
                QueryableStoreTypes.keyValueStore[Int, LikesAvg]()
              )
          )

          val results = new scala.collection.mutable.ListBuffer[(Int, LikesAvg)]()

          val iter = store.all()
          while (iter.hasNext) {
            val next = iter.next()
            results += ((next.key, next.value))
          }
          iter.close()

          complete(StatusCodes.OK, results.toList)
        }
      }
    )
  }
}
