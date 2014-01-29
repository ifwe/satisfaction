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
object PlumbGraph {

    case class NodeDiv(
        val divContent: String = "",
        val divClass: String = "window",
        val divStyle: String = "" ) {

        var id: String = "0"
        var height: Int = 0
        var width: Int = 0
        var posX: Int = 0
        var posY: Int = 0
        var color: String = null
        var onClick: String = null

        def toHtml() = {
            "<div id=\"" + id + "\" class=\"" + divClass + "\" " +
                "style=\"" + genDivStyle + "\"" + 
                { if( onClick != null) { " onClick=\"" + onClick + "\" " } else { "" } } +
                ">" + divContent + "</div>"
        }

        def genDivStyle = {
            var st = divStyle
            if (height != 0)
                st += "height:" + height.toString + "em;"
            if (width != 0)
                st += "width:" + width.toString + "em;"
            if (posX != 0)
                st += "left:" + posX.toString + "em;"
            if (posY != 0)
                st += "top:" + posY.toString + "em;"
            if (color != null)
                st += "background-color:" + color + ";"

            st
        }
    }

    case class Connection(
        source: NodeDiv,
        target: NodeDiv) {

        /// XXXX Todo ... Allow for more overlay logic
        /// to specify different sorts of arrows
        def toJavaScript: String = {
            " jsPlumb.connect({ source:\"" + source.id + "\", target:\"" + target.id + "\"," +
                "  paintStyle:{ strokeStyle:\"blue\", lineWidth:3 }, " +
                "  overlays:[ [\"Arrow\" , { width:15, height:18, location:1 } ] ] " +
                " }); ";
        }
    }

}
case class PlumbGraph() {
    var divs: List[PlumbGraph.NodeDiv] = List()
    var connections: List[PlumbGraph.Connection] = List()
    var idCnt = 0

    var divWidth = 500
    var divHeight = 500

    def generateJavaScript(): String = {
        connections.map(conn => conn.toJavaScript) mkString ("\n")
    }

    def generateHtmlDivs(): String = {
        divs.map(node => node.toHtml).mkString("\n")
    }

    def addNodeDiv(node: PlumbGraph.NodeDiv): PlumbGraph = {
        node.id = idCnt.toString
        idCnt += 1

        divs :+= node
        this
    }

    def addConnection(source: PlumbGraph.NodeDiv, target: PlumbGraph.NodeDiv): PlumbGraph = {
        connections :+= PlumbGraph.Connection(source, target)
        if (!divs.contains(source))
            divs :+= source

        if (!divs.contains(target))
            divs :+= target

        this
    }
}