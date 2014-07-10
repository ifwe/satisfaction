package com.klout
package satisfaction

import collection._
import collection.JavaConversions._


/**
 *  A Witness represents a set of Variable Assignments.
 *  
 *  A Variable consists of a Variable name, and a type.
 *  A VariableAssignment consists of a Variable, 
 *    and an Object of that type.
 */
case class Variable[T](val name: String, val clazz: Class[T], val description: Option[String] = None) {
    def ->(t: T): VariableAssignment[T] = VariableAssignment(this, t)

    override lazy val toString =
        s"name=[$name], class=[$clazz], description=[${description getOrElse ""}]"
    
    override def equals( other : Any ) : Boolean = {
      //// assume just the names are the same
      if( other.isInstanceOf[Variable[_]]) {
    	  val otherVar = other.asInstanceOf[Variable[_]]
    	  name.equals( otherVar.name)
      } else {
        false
      }
    }
    
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

case class VariableAssignment[T](val variable: Variable[T], val value: T) {
    lazy val raw: (String, String) = variable.name -> value.toString

    override def toString : String = {
        s"(${variable.name} => $value)" 
    }
}

object VariableAssignment {
    def apply[T](name: String, value: T)(implicit m: Manifest[T]): VariableAssignment[T] = {
        new VariableAssignment(new Variable[T](name, m.runtimeClass.asInstanceOf[Class[T]]), value)
    }

    def apply[T](name: String, value: T, desc: String)(implicit m: Manifest[T]): VariableAssignment[T] = {
        new VariableAssignment(new Variable[T](name, m.runtimeClass.asInstanceOf[Class[T]], Some(desc)), value)
    }
    
    val ordering = new Ordering[VariableAssignment[_]] {
       override def compare( va1 : VariableAssignment[_], va2 : VariableAssignment[_]) : Int = {
        va1.variable.name.compareTo( va2.variable.name)
       }
      
    }
}

case class Witness(
    val assignments: Set[VariableAssignment[_]]) {

    lazy val raw: immutable.Map[String, String] = assignments map (_.raw) toMap
    
    lazy val variables: Set[Variable[_]] = assignments.map(_.variable).toSet


    def ++(other: Witness): Witness = {
        (this /: other.assignments)(_ + _)
    }
    
    def filter( vars : Set[Variable[_]]) : Witness = {
       new Witness( assignments filter( ass => { vars.contains( ass.variable) } ) )
    }
    
    def exclude( vars : Set[Variable[_]]) : Witness = {
       new Witness( assignments filter( ass => { ! vars.contains( ass.variable) } ) )
    }

    def get[T](param: Variable[T]): Option[T] =
        assignments find (_.variable == param) map (_.value.asInstanceOf[T])

    def update[T](param: Variable[T], value: T): Witness = {
        val filtered = assignments filterNot (_.variable == param)
        new Witness(filtered + (param -> value))
    }

    def contains[T](variable: Variable[T]): Boolean = {
        assignments.map(_.variable).contains(variable)
    }
    
    def contains( variable : String ) : Boolean = {
        assignments.map(_.variable).contains(Variable(variable))
    }

    def +[T](param: Variable[T], value: T): Witness =
        update(param, value)

    def +[T](pair: VariableAssignment[T]): Witness =
        update(pair.variable, pair.value)

    def update[T](pair: VariableAssignment[T]): Witness = update(pair.variable, pair.value)

    def mkString(start: String,  delim : String, end: String ) : String = {
       val sortedSet = SortedSet[VariableAssignment[_]]()( ord =  VariableAssignment.ordering ) ++ assignments
       sortedSet.mkString(start,delim,end)
    }

    def mkString( delim : String ) : String =  mkString( "", delim, "") 

    override def toString : String = {
      mkString(";")
    }
    
    def pathString : String = {
      mkString("_").replace(" ","_").replace("=>","@").replace("(","_").replace(")","_")
    }
}

object Witness {
    def apply(pairs: VariableAssignment[_]*): Witness = new Witness(pairs.toSet)

    def apply(properties: Map[String, String]): Witness = {
        val assignSet = properties collect { case (k, v) => new VariableAssignment[String](Variable[String](k, classOf[String]), v) }
        new Witness(assignSet.toSet)
    }

    val empty = new Witness(Set.empty)

    implicit def Witness2Properties( subst : Witness ) : java.util.Properties = {
       val props = new java.util.Properties      
       subst.raw.foreach { case (k, v) => {
           props.setProperty( k, v.toString) 
         } 
       }
       props
    }
    
    implicit def Properties2Witness( props : java.util.Properties ) : Witness = {
        new Witness( props.entrySet.map( entry =>  {
          val keyVar : Variable[String] = Variable( entry.getKey.toString )
           new VariableAssignment[String]( keyVar, entry.getValue.toString )
        } ) )
    }
    
    
   /**
    *   Define a function which maps one variable to another.
    *   Useful for Goal dependencies, if one table refers to 
    *    a variable by a different name.
    */ 
    def mapVariables[T]( vfrom : Variable[T], vto : Variable[T] )( fromWitness : Witness) : Witness = {
      val oldValCheck = fromWitness.get( vfrom)
      oldValCheck match {
        case Some(oldVal) => {
          fromWitness.exclude( Set(vfrom)) + ( vto, oldVal)
        } 
        case None => fromWitness
      }
   }
    
}

