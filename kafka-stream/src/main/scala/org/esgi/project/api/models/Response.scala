package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class Score(
   id: Int,
   title: String,
   score: Double
 )


// Responses
case class TenBestScore(
   aggregation: List[Score]
)

// Implicits
object Score {
  implicit val format: OFormat[Score] = Json.format[Score]
}


object TenBestScore {
  implicit val format: OFormat[TenBestScore] = Json.format[TenBestScore]
}
