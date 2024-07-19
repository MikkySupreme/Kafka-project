package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}


case class LikesAvg(
   count: Long,
   sum: Double,
   avg: Double
) {
  def increment(score: Double) = this.copy(sum = this.sum + score, count = this.count + 1).computeAvg

  def computeAvg: LikesAvg = this.copy(avg = this.sum / this.count)

}
object LikesAvg {
  implicit val format: OFormat[LikesAvg] = Json.format[LikesAvg]

  def empty: LikesAvg = LikesAvg(0, 0, 0)
}