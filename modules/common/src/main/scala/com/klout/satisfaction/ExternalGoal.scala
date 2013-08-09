package com.klout.satisfaction

/**
 *  ExternalDataGoal 
 *    represents the output of a different project
 */
class ExternalDataGoal(
      val externalData : DataOutput
    ) extends Goal(
        externalData.toString,
        WaitForData,
        externalData.getVariables,
        null,
        Set( externalData)
    ) {
  
  val minimumSize = 64*1024*1024
  val sleepUntil = 8*60*60*1000
  val sleepInterval = 60*1000

}