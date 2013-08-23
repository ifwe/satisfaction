package com.klout.satisfaction

/**
 *  Specialized context class
 *   for Goals
 *
 */
case class Witness(val substitution: Substitution) {

    lazy val variables: Set[Variable[_]] = substitution.assignments.map(_.variable).toSet

    def update[T](assignment: VariableAssignment[T]): Witness = this copy (substitution + assignment)
}

object Witness {
    def apply(pairs: VariableAssignment[_]*): Witness = Witness(Substitution(pairs: _*))

    def apply(variables: Set[Variable[_]], substitution: Substitution): Witness = {
        new Witness(new Substitution(substitution.assignments.filter(x => variables.contains(x.variable))))
    }
}