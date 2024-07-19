package org.esgi.project.api.models

import org.esgi.project.streaming.models.ViewPerCategory
import play.api.libs.json.{Json, OFormat}

case class Score(
   id: Int,
   title: String,
   score: Double
 )
case class View(
                   id: Int,
                   title: String,
                   views: Long
                 )

object View {
  implicit val format: OFormat[View] = Json.format[View]
}

case class Movie(
  id: Int,
  title: String,
  total_view_count: Long,
  past: ViewPerCategory,
  last_five_minutes: ViewPerCategory
)
case class ViewList(
                         views: List[View]
                       )

object ViewList {
  implicit val format: OFormat[ViewList] = Json.format[ViewList]
}

case class ScoreList(
  scores: List[Score]
)

object Score {
  implicit val format: OFormat[Score] = Json.format[Score]
}


object ScoreList {
  implicit val format: OFormat[ScoreList] = Json.format[ScoreList]
}

object Movie {
  implicit val format: OFormat[Movie] = Json.format[Movie]
}
