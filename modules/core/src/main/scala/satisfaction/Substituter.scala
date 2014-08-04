package satisfaction

import scala.util.parsing.input.Reader
import scala.util.parsing.input.CharSequenceReader
import scala.collection.mutable.Buffer
import java.io.InputStream

/**
 * Read a String with ${variables} and output the lines with the values substituted
 */
object Substituter {

    def findVariablesInString(templateStr: String): List[String] = {
        substitute(templateStr, Witness()) match {
            case Left(vars)    => vars
            case Right(noVars) => List.empty
        }
    }

    def substitute(str: String, subst: Witness): Either[List[String], String] = {
        return substitute(new CharSequenceReader(str), subst)
    }

  def substitute(readerBegin: CharSequenceReader, subst: Witness): Either[List[String], String] = {
    try {
      val sb: StringBuilder = new StringBuilder
      val missingList: Buffer[String] = new collection.mutable.ArrayBuffer()

      var reader = readerBegin
      while ({ !reader.atEnd }) {
        reader.first match {
          case '$' =>
            val nextChar = reader.rest.first
            if (nextChar == '{') {

              readVar(reader.rest.rest) match {
                case Left(malformed) =>
                  missingList += malformed
                case Right(varTuple) =>
                  val varName = varTuple._1
                  reader = varTuple._2
                  subst.get(Variable(varName)) match {
                    case Some(lookup) =>
                      sb ++= lookup
                    case None => missingList += varName
                  }
              }
            } else {
              sb += '$'
            }

          case v =>
            sb += v

        }
        reader = reader.rest
      }
      if (missingList.size != 0)
        Left(missingList.toList)
      else
        Right(sb.toString)
    } catch {
      case unexpected: Throwable =>
        println(" Unexpedted error in substitures " + unexpected)
        unexpected.printStackTrace()
        throw unexpected
    }
  }

    def isValidCharacter(ch: Char): Boolean = {
        ch.isLetterOrDigit || ch == '_'
    }

    /**
     *  Read a variable
     */
    private def readVar(readerBegin: CharSequenceReader): Either[String, Tuple2[String, CharSequenceReader]] = {
        val sbVar = new StringBuilder
        var reader = readerBegin
        while ({ reader.first != '}' && !reader.atEnd }) {
            if (isValidCharacter(reader.first)) {
                sbVar += reader.first
                reader = reader.rest
            } else {
                return Left(sbVar.toString)
            }
        }
        if (reader.atEnd)
            return Left(sbVar.toString)
        Right(Tuple2(sbVar.toString, reader))
    }

    /**
     *  Substitute any variables
     */
    def substituteVarsInMap(varMap: Map[String, String]): Map[String, String] = {
        varMap.collect {
            case (k, v) =>
                substitute(new CharSequenceReader(v), Witness(varMap)) match {

                    case Left(extraVars) =>
                        /// There are some missing variables which need to be defined elsewhere ...
                        /// XXXX Barf on missing vars ???
                        ///throw new IllegalArgumentException(s" Missing variables is property $k with value $v -- " + extraVars)
                        println(s" Missing variables is property $k with value $v -- " + extraVars)
                        ("" -> "")
                    case Right(line) =>
                        (k -> line)
                }
        }
    }

    def readProperties(propertyFile: String): Map[String, String] = {
       readProperties( io.Source.fromFile( propertyFile)) 
    } 
    
    def readProperties( strm : InputStream): Map[String, String] = {
       readProperties( io.Source.fromInputStream( strm)) 
    } 
    
    def readProperties( source : io.BufferedSource): Map[String, String] = {
        val fileProps = source.getLines.filterNot(_.isEmpty).
            filterNot(_.startsWith("#")).collect{ case s: String => parseLine(s) }.toMap

        substituteVarsInMap(fileProps)
    }

    private def parseLine(propertyString: String): Tuple2[String, String] = {
        val property: Array[String] = propertyString.split("=")
        if (property.length != 2)
            throw new RuntimeException(s"error: property file not correctly formatted :: $propertyString ")

        property(0).trim -> property(1).trim
    }

}