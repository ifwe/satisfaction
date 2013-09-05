package hive.ms

import org.apache.hadoop.hive.ql.parse.ParseDriver
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.ql.parse.ParseException
import scala.collection.JavaConversions._
import com.klout.satisfaction.DataOutput
import com.klout.satisfaction.DataInstance
import com.klout.satisfaction.HiveTablePartition
import org.apache.hadoop.hive.ql.tools.LineageInfo
import org.apache.hadoop.hive.ql.optimizer.index.RewriteParseContextGenerator

object HiveParser {

    val parseDriver = new org.apache.hadoop.hive.ql.parse.ParseDriver
    val config = Config.config

    def parseSyntax(query: String): Boolean = {
        try {
            val astNode = parseDriver.parse(query)

            true
        } catch {
            case parseExc: ParseException =>
                false
            case e: Throwable =>
                throw e
        }
    }

    /**
     * Analyze the query, and return the output
     */
    def analyzeQuery(query: String): Set[DataInstance] = {
        val parseCtxt: ParseContext = RewriteParseContextGenerator.generateOperatorTree(config, query)
        val lineage = parseCtxt.getLineageInfo()
        println(" Lineageis " + lineage)
        val readEntities = parseCtxt.getSemanticInputs.toSet
        val depends = for (ent <- readEntities) yield {
            val part = ent.getPartition()
            println(" Partition is " + part.getCompleteName())
            ////new HiveTablePartition(part,ms)
            new HiveTablePartition(part)
        }
        depends.toSet[DataInstance]
    }
    /**
     * val astNode = ParseUtils.findRootNonNullToken(parseDriver.parse(query))
     * System.out.println(" ASTNode = " + astNode)
     * val semanticAnalyzer = SemanticAnalyzerFactory.get(config, astNode)
     * println(" Semantic Analyzer  = " + semanticAnalyzer)
     * semanticAnalyzer.validate()
     * val outputs = semanticAnalyzer.getOutputs().toSet
     * val partitions: Set[DataInstance] = for (writeEntity <- outputs) yield {
     * System.out.println("Write Entity = " + writeEntity.getName())
     * println("Write Entity = " + writeEntity.getName())
     * val part = writeEntity.getPartition
     * val tbl = writeEntity.getTable()
     * new HiveTablePartition(part)
     * }
     *
     * partitions
     * }
     *
     */

    def getLineage(query: String) = {
        val lineage = new LineageInfo()
        lineage.getLineageInfo(query)
        lineage.getInputTableList.foreach(inp => println(" Input Table " + inp))
        lineage.getOutputTableList.foreach(out => println(" Outpput Table " + out))
    }

}
