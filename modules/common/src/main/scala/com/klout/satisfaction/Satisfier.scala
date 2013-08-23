package com.klout.satisfaction

trait Satisfier {

    def satisfy(subst: Substitution): Boolean

}
