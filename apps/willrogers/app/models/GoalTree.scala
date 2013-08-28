package models

case class GoalTree(
    depends: Set[GoalTree]) {

}