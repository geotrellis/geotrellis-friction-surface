package ingest

import vectorpipe._
import squants.motion.Velocity
import scala.util._

object OsmHighwaySpeed {
  /** Parse OSM highway element to derive max speed in km/h */
  def apply(feature: osm.ElementMeta): Option[Double] = {
    val highway: Option[String] = feature.tags.get("highway")
    val maxspeed: Option[String] = feature.tags.get("maxspeed")
    val kph: Option[Double] =
      highway.flatMap { highway: String =>
        maxspeed.flatMap { speed: String =>
          Velocity(speed).toOption
            .map(_.toKilometersPerHour)
            .orElse(CountrySpeeds.get(speed))
            .orElse(DefaultSpeeds.get(highway))
            .orElse(DefaultSpeeds.get("road"))
        }
      }

    if (kph.isEmpty) {
      println(s"PARSE ERROR: highway: $highway maxspeed: $maxspeed")
      Some(DefaultSpeeds("road"))
    } else {
      kph
    }
  }

  val DefaultSpeeds: Map[String, Double] = Map(
    "motorway" -> 100,
    "trunk" -> 70,
    "primary" -> 65,
    "secondary" -> 60,
    "tertiary" -> 50,
    "unclassified" -> 30,
    "residential" -> 30,
    "service" -> 20,
    "motorway_link" -> 70,
    "trunk_link" -> 65,
    "primary_link" -> 60,
    "secondary_link" -> 50,
    "tertiary_link" -> 40,
    "living_street" -> 10,
    "pedestrian" -> 6,
    "track" -> 20,
    "road" -> 35)

  val CountrySpeeds: Map[String, Double] = Map(
    "at:urban" -> 50,
    "at:rural" -> 100,
    "at:trunk" -> 100,
    "at:motorway" -> 130,
    "ch:urban" -> 50,
    "ch:rural" -> 80,
    "ch:trunk" -> 100,
    "ch:motorway" -> 120,
    "cz:urban" -> 50,
    "cz:rural" -> 90,
    "cz:trunk" -> 130,
    "cz:motorway" -> 130,
    "dk:urban" -> 50,
    "dk:rural" -> 80,
    "dk:motorway" -> 130,
    "de:living_street" -> 7,
    "de:urban" -> 50,
    "de:walk" -> 7,
    "de:rural" -> 100,
    "fi:urban" -> 50,
    "fi:rural" -> 80,
    "fi:trunk" -> 100,
    "fi:motorway" -> 120,
    "fr:urban" -> 50,
    "fr:rural" -> 90,
    "fr:trunk" -> 110,
    "fr:motorway" -> 130,
    "hu:urban" -> 50,
    "hu:rural" -> 90,
    "hu:trunk" -> 110,
    "hu:motorway" -> 130,
    "it:urban" -> 50,
    "it:rural" -> 90,
    "it:trunk" -> 110,
    "it:motorway" -> 130,
    "jp:national" -> 60,
    "jp:motorway" -> 100,
    "ro:urban" -> 50,
    "ro:rural" -> 90,
    "ro:trunk" -> 100,
    "ro:motorway" -> 130,
    "ru:living_street" -> 20,
    "ru:rural" -> 90,
    "ru:urban" -> 60,
    "ru:motorway" -> 110,
    "sk:urban" -> 50,
    "sk:rural" -> 90,
    "sk:trunk" -> 130,
    "sk:motorway" -> 130,
    "si:urban" -> 50,
    "si:rural" -> 90,
    "si:trunk" -> 110,
    "si:motorway" -> 130,
    "se:urban" -> 50,
    "se:rural" -> 70,
    "se:trunk" -> 90,
    "se:motorway" -> 110,
    "gb:nsl_single" -> 96.54,
    "gb:nsl_dual" -> 112.63,
    "gb:motorway" -> 112.63,
    "ua:urban" -> 60,
    "ua:rural" -> 90,
    "ua:trunk" -> 110,
    "ua:motorway" -> 130,
    "living_street" -> 6)
}
