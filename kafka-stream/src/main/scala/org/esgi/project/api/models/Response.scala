package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class Score(
                  id: Int,
                  title: String,
                  score: Double
                )

case class Viewed(
                   id: Int,
                   title: String,
                   views: Long
                 )

case class TenBestScore(
                         aggregation: List[Score]
                       )

case class TenBestViews(
                         aggregation: List[Viewed]
                       )

object Score {
  implicit val format: OFormat[Score] = Json.format[Score]
}

object Viewed {
  implicit val format: OFormat[Viewed] = Json.format[Viewed]
}


object TenBestScore {
  implicit val format: OFormat[TenBestScore] = Json.format[TenBestScore]
}

object TenBestViews {
  implicit val format: OFormat[TenBestViews] = Json.format[TenBestViews]
}
