package com.klout.satisfaction

trait WitnessGenerator {

    def generate(current: Option[Witness]): Option[Witness]

}

object WitnessGenerator {
    def apply(f: Option[Witness] => Option[Witness]) = new WitnessGenerator {
        override def generate(current: Option[Witness]): Option[Witness] = f(current)
    }
}
