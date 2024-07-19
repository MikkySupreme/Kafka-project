package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class ViewPerCategory(
  start_only: Long,
  half: Long,
  full: Long
){
  def increment(category: String): ViewPerCategory = category match {
    case "start_only" => this.copy(start_only = this.start_only + 1)
    case "half" => this.copy(half = this.half + 1)
    case "full" => this.copy(full = this.full + 1)
    case _ => this
  }
}

object ViewPerCategory {
  implicit val format: OFormat[ViewPerCategory] = Json.format[ViewPerCategory]
  def empty: ViewPerCategory = ViewPerCategory(0L, 0L, 0L)
}
