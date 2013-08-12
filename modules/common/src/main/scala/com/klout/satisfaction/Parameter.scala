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

    import org.joda.time._
    implicit val LocalDateParamable = Paramable[LocalDate](_ toString "yyyyMMdd")
}

class ParamOverrides(private[satisfaction] val map: Map[Param[_], Map[Any, Set[ParamPair[_]]]])

object ParamOverrides {

    def forParam[T: Paramable](param: Param[T]) = new {
        def set(t: (T, Set[ParamPair[_]])*): ParamOverrides = {
            val newValue: Map[Any, Set[ParamPair[_]]] = t.toMap
            val newMap: Map[Param[_], Map[Any, Set[ParamPair[_]]]] = Map(param -> newValue)
            new ParamOverrides(newMap)
        }
    }
}

object Foo {
    object Date extends Param[String]("dt")
    object Network extends Param[String]("network_abbr")
    object Foo extends Param[String]("foo")
    object Bar extends Param[String]("bar")
    object Baz extends Param[String]("baz")

    ParamOverrides forParam Date set (
        "today" -> Set(Foo -> "foo1", Bar -> "bar2"),
        "yesterday" -> Set(Foo -> "foo2", Bar -> "barasdf")
    )
}