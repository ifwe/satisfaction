package com.klout.satisfaction

import scala.language.reflectiveCalls

abstract class Param[T: Paramable](val name: String, val description: Option[String] = None) {
    def ->(t: T): ParamPair[T] = ParamPair(this, t)
}

case class ParamPair[T: Paramable](param: Param[T], value: T) {
    lazy val raw: (String, String) = param.name -> (Paramable toParam value)
}

class ParamMap(pairs: Set[ParamPair[_]]) {
    lazy val raw: Map[String, String] = pairs map (_.raw) toMap

    def withOverrides(overrides: ParamOverrides): ParamMap = {
        var newMap = this

        for {
            (param, rules) <- overrides.map
            currentValue <- newMap get param
            overrides <- rules get currentValue
            ParamPair(param, value) <- overrides
        } {
            newMap = this update (param, value)
        }

        newMap
    }

    def get[T](param: Param[T]): Option[T] =
        pairs find (_.param == param) map (_.asInstanceOf[T])

    def update[T](param: Param[T], value: T): ParamMap = {
        val filtered = pairs filterNot (_.param == param)
        new ParamMap(pairs + (param -> value))
    }

}

object ParamMap {
    def apply(pairs: ParamPair[_]*): ParamMap = new ParamMap(pairs.toSet)
}

trait Paramable[T] {
    def toParam(t: T): String
}

object Paramable {
    def toParam[T: Paramable](t: T): String = implicitly[Paramable[T]] toParam t

    def apply[T](f: T => String): Paramable[T] = new Paramable[T] {
        override def toParam(t: T): String = f(t)
    }

    implicit val StringParamable = Paramable[String](_.toString)
    implicit val IntParamable = Paramable[Int](_.toString)
    implicit val BooleanParamable = Paramable[Boolean](_.toString)

    import org.joda.time._
    implicit val LocalDateParamable = Paramable[LocalDate](_ toString "yyyyMMdd")
}

class ParamOverrides(private[satisfaction] val map: Map[Param[_], Map[Any, Set[ParamPair[_]]]])

object ParamOverrides {

    def apply[T: Paramable](param: Param[T]) = new {
        def set(t: (T, Set[ParamPair[_]])*): ParamOverrides = {
            val newValue: Map[Any, Set[ParamPair[_]]] = t.toMap
            val newMap: Map[Param[_], Map[Any, Set[ParamPair[_]]]] = Map(param -> newValue)
            new ParamOverrides(newMap)
        }
    }

}

object example {
    object NetworkAbbr extends Param[String]("network_abbr")
    object DoDistcp extends Param[Boolean]("doDistcp")

    val overrides = ParamOverrides(NetworkAbbr) set (
        "li" -> Set(DoDistcp -> true),
        "tw" -> Set(DoDistcp -> false)
    )
}
