package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class Likes(
  id: String,
  score: String
)



object Likes {
  implicit val format: OFormat[Likes] = Json.format[Likes]
}
