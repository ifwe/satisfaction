package com.klout.satisfaction
package common
package dsl

import org.joda.time._
import play.api.libs.json._

object `package` {
    def ???[T]: T = sys.error("Not implemented yet!")
}

trait Satisfier {
    def satisfyMe(goalPeriod: GoalContext)
    def amISatisfied(goalPeriod: GoalContext): Boolean
}

object Satisfier {
    implicit val fmt = ???
}

sealed trait DataOutput

object DataOutput {
    implicit val hiveTableFmt = Json.format[HiveTable]
    implicit val hdfsPathFmt = Json.format[HdfsPath]

    def unapply(unapplied: DataOutput): Option[(String, JsValue)] = {
        val (prod: Product, sub: JsValue) = unapplied match {
            case b: HiveTable => (b, Json.toJson(b)(hiveTableFmt))
            case b: HdfsPath  => (b, Json.toJson(b)(hdfsPathFmt))
        }
        Some(prod.productPrefix -> sub)
    }

    def apply(`class`: String, data: JsValue): DataOutput = {
        (`class` match {
            case "HiveTable" => Json.fromJson[HiveTable](data)
            case "HdfsPath"  => Json.fromJson[HdfsPath](data)
        }).get
    }

    implicit val fmt = Json.format[DataOutput]
}

case class HiveTable(
    name: String,
    constantParams: Map[String, String] = Map.empty,
    variableParams: Set[String] = Set.empty) extends DataOutput

case class HdfsPath(
    path: String,
    constantParams: Map[String, String] = Map.empty,
    variableParams: Set[String] = Set.empty) extends DataOutput

case class InternalGoal(
    name: String,
    satisfier: Satisfier,
    constantParams: Map[String, String] = Map.empty,
    variableParams: Set[String] = Set.empty,
    dependsOn: Set[InternalGoal],
    externalDependsOn: Set[ExternalGoal],
    outputs: Set[DataOutput])

case class ExternalGoal(
    dependsOn: Set[DataOutput],
    variableParams: Set[String])

case class GoalContext(
    name: String,
    created: DateTime,
    params: Map[String, String])

object GoalContext {
    implicit val fmt = Json.format[GoalContext]
}

sealed trait GoalContextGenerator

object GoalContextGenerator {
    implicit val dailyFmt = Json.format[DailyGoalContextGenerator]

    def unapply(unapplied: GoalContextGenerator): Option[(String, JsValue)] = {
        val (prod: Product, sub: JsValue) = unapplied match {
            case b: DailyGoalContextGenerator => (b, Json.toJson(b)(dailyFmt))
        }
        Some(prod.productPrefix -> sub)
    }

    def apply(`class`: String, data: JsValue): GoalContextGenerator = {
        (`class` match {
            case "GoalContextGenerator" => Json.fromJson[GoalContextGenerator](data)
        }).get
    }

    implicit val fmt = Json.format[GoalContextGenerator]
}

case class DailyGoalContextGenerator(hour: Int, minute: Int) extends GoalContextGenerator

case class Project(
    name: String,
    goalContextGenerator: GoalContextGenerator,
    goals: Set[InternalGoal] ///dependencies: Set[ExternalGoal]
    ) {
    lazy val externalGoals = {

    }
}

abstract class ProjectProvider(val project: Project)
