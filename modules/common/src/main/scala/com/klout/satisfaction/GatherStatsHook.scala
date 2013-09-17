package com.klout.satisfaction

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext
import org.apache.hadoop.hive.ql.hooks.Hook
import org.apache.hadoop.hive.ql.hooks.HookContext
import scala.collection.JavaConversions._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.mapred.JobTracker
import java.io.DataOutputStream
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.TableIdEnum
import org.apache.hadoop.mapred.Task.Counter._
import org.apache.hadoop.hive.ql.hooks.Entity

/**
 *  After Hive task is run, gather the
 *     statistics to update metadata,
 *     and provide a simple sanity-check mechaniskm
 */
class GatherStatsHook extends ExecuteWithHookContext with Hook {
    lazy implicit val ms: hive.ms.MetaStore = hive.ms.MetaStore

    /**
     *  Mapping of Counter Enum types to Hive MetaData keys
     *    to be stored
     */
    val CounterMapping = Map(
        TableIdEnum.TABLE_ID_1_ROWCOUNT -> "NUM_ROWS",
        org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_RECORDS -> "MAP_INPUT_RECORDS",
        org.apache.hadoop.mapred.Task.Counter.SPILLED_RECORDS -> "SPILLED_RECORDS"
    )

    override def run(hookContext: HookContext): Unit = {
        println(" GatherStatsHook " + hookContext)
        ///println(" Operation is " + hookContext.getOperationName)
        if (hookContext.getOutputs.size > 0) {
            hookContext.getOutputs.toSeq.foreach(out => {
                println(" Output is " + out.getName)
            })
        }

        if (hookContext.getCompleteTaskList.size() > 0) {
            val lastTask = hookContext.getCompleteTaskList().last
            println(" Last Task was " + lastTask.getId() + " Name was " + lastTask.getName)
            println(" Last Task Task " + lastTask.getTask)
            if (lastTask.getTask() != null) {
                println(" TASK Job id " + lastTask.getTask.getJobID())
                println(" TASK Job id " + lastTask.getTask.getId())
                lastTask.getTask.getCounters.toMap.foreach{
                    case (k, v) =>
                        println(k + " == " + v)
                }
            }
        }

        val sessionState = SessionState.get
        println(" Session state command = " + sessionState.getCmd())

        val mapRedStats = sessionState.getLastMapRedStatsList
        if (mapRedStats.size() > 0) {
            val lastStat = mapRedStats.last
            println(" Last MadRed Stats = " + lastStat.getJobId() + " Num Maps = " + lastStat.getNumMap() + " num reducers =  " + lastStat.getNumReduce())
            println(" number of rows in table " + lastStat.getCounters.getCounter(
                TableIdEnum.TABLE_ID_1_ROWCOUNT))
            lastStat.getCounters.write(new DataOutputStream(System.out))

            val dataOutOpt = getOutput(hookContext)
            CounterMapping.foreach {
                case (eenum, mappedName) => {
                    val counterVal = lastStat.getCounters.getCounter(eenum)
                    println(s" Value of Counter $mappedName is $counterVal ")
                    dataOutOpt match {
                        case Some(dataOut) =>
                            if (counterVal != null) {
                                ms.setInstanceMetaData(dataOut, mappedName, counterVal.toString)
                            }
                    }
                }
            }

        }

    }

    /**
     *  Examine the outputs, to see if a Table or Partition
     *   is being created with this statement.
     */
    def getOutput(hookContext: HookContext): Option[DataInstance] = {
        if (hookContext.getOutputs.size > 0) {
            hookContext.getOutputs.toSeq.foreach(out => {
                println(" Output is " + out.getName)
            })
            val writeEntity = hookContext.getOutputs.last
            writeEntity.getTyp match {
                case Entity.Type.PARTITION =>
                    return Some(new HiveTablePartition(writeEntity.getP))
            }
            return None
        } else {
            return None
        }
    }

}