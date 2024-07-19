package org.esgi.project.api.models

import org.esgi.project.streaming.models.ViewPerCategory
import play.api.libs.json.{Json, OFormat}

case class Score(
   id: Int,
   title: String,
   score: Double
 )

case class Movie(
  id: Int,
  title: String,
  total_view_count: Long,
  past: ViewPerCategory,
  last_five_minutes: ViewPerCategory
)

// Responses
case class ScoreList(
  scores: List[Score]
)

// Implicits
object Score {
  implicit val format: OFormat[Score] = Json.format[Score]
}


object ScoreList {
  implicit val format: OFormat[ScoreList] = Json.format[ScoreList]
}

object Movie {
  implicit val format: OFormat[Movie] = Json.format[Movie]
}
