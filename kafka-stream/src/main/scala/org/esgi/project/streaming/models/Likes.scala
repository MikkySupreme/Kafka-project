package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

import java.util.UUID

case class Likes(
    id: Int,
    score: Double,
)


object Likes {
  implicit val format: OFormat[Likes] = Json.format[Likes]
}
