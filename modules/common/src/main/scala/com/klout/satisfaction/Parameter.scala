package com.klout.satisfaction

import collection._

case class Variable[T](val name: String, val clazz: Class[T], val description: Option[String] = None) {
    def ->(t: T): VariableAssignment[T] = VariableAssignment(this, t)

    override lazy val toString =
        s"name=[$name], class=[$clazz], description=[${description getOrElse ""}]"
    
    
}
object Variable {
    //// Assume it is a string type if not defined
    def apply(name: String, description: String): Variable[String] = {
        new Variable(name, classOf[String], Some(description))
    }
    def apply(name: String): Variable[String] = {
        new Variable(name, classOf[String])
    }

    
}

case class VariableAssignment[T](variable: Variable[T], value: T) {
    lazy val raw: (String, String) = variable.name -> value.toString

}
object VariableAssignment {
    def apply[T](name: String, value: T)(implicit m: Manifest[T]): VariableAssignment[T] = {
        new VariableAssignment(new Variable[T](name, m.runtimeClass.asInstanceOf[Class[T]]), value)
    }

    def apply[T](name: String, value: T, desc: String)(implicit m: Manifest[T]): VariableAssignment[T] = {
        new VariableAssignment(new Variable[T](name, m.runtimeClass.asInstanceOf[Class[T]], Some(desc)), value)
    }
}

class Substitution(
    val assignments: Set[VariableAssignment[_]]) {

    lazy val raw: immutable.Map[String, String] = assignments map (_.raw) toMap

    def ++(other: Substitution): Substitution = {
        (this /: other.assignments)(_ + _)
    }

    /**
     * def ++(overrides: ParamOverrides): Substitution =
     * withOverrides(overrides)
     *
     */

    def get[T](param: Variable[T]): Option[T] =
        assignments find (_.variable == param) map (_.value.asInstanceOf[T])

    def update[T](param: Variable[T], value: T): Substitution = {
        val filtered = assignments filterNot (_.variable == param)
        new Substitution(filtered + (param -> value))
    }

    def contains[T](variable: Variable[T]): Boolean = {
        assignments.map(_.variable).contains(variable)
    }

    def +[T](param: Variable[T], value: T): Substitution =
        update(param, value)

    def +[T](pair: VariableAssignment[T]): Substitution =
        update(pair.variable, pair.value)

    def update[T](pair: VariableAssignment[T]): Substitution = update(pair.variable, pair.value)

}

object Substitution {
    def apply(pairs: VariableAssignment[_]*): Substitution = new Substitution(pairs.toSet)

    def apply(properties: Map[String, String]): Substitution = {
        val assignSet = properties collect { case (k, v) => new VariableAssignment[String](Variable[String](k, classOf[String]), v) }
        new Substitution(assignSet.toSet)
    }

    val empty = new Substitution(Set.empty)

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

