package models

/**
 * Simple model to create jsPlumb graphs
 *  For now, create server side object
 *   which maps onto necessary div's,
 *   and generates javascript jsPlumb commands to
 *    draw static graph .
 *
 *  Assumes all CSS styles exist in play template,
 *    so it can easily be placed into the front-end
 *
 *
 *  TODO --
 *    Change to an AJAX service, which outputs
 *     a JSON representation, and write a
 *    layer in either javascript/Node.js
 *     which makes call to the service to get the appropriate
 *    Graph
 */
class PlumbGraph {
    var divs: List[NodeDiv] = List()
    var connections: List[Connection] = List()

    case class NodeDiv(
        val id: String,
        val divClass: String = "window",
        val divStyle: String = "",
        val divContent: String = "") {

        def toHtml() = {
            "<div id=\"" + id + "\" class=\"" + divClass
            "style=\"" + divStyle + "\">" + divContent + "</div>"
        }

    }

    case class Connection(
        source: NodeDiv,
        target: NodeDiv) {

        def toJavaScript: String = {
            " jsPlumb.connect({ source:\"#" + source.id + "\", target:\"#" + target.id + "\"});"
        }
    }

    def generateJavaScript(): String = {
        connections.map(conn => conn.toJavaScript) mkString ("\n")
    }

    def generateHtmlDivs(): String = {
        divs.map(node => node.toHtml).mkString("\n")
    }

    def addNodeDiv(node: NodeDiv) = {
        divs :+= node
    }

    def addConnection(source: NodeDiv, target: NodeDiv) = {
        connections :+= Connection(source, target)
        if (!divs.contains(source))
            divs :+= source

        if (!divs.contains(target))
            divs :+= target
    }
}