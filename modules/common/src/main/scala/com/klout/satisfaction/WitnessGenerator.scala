package com.klout.satisfaction

trait WitnessGenerator {

    def generate(): Option[Witness]
}