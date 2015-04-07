package models

import satisfaction._
import collection._
import play.api.mvc.Request
import play.api.mvc.AnyContent

case class VariableHolder(val dt: String, val network_abbr: String, val service_id: String) {

}

/**
 *  Since witnesses can have arbitrary types,
 *     and have arbitrary lengths and names
 */
class VariableFormHandler(val variables: List[Variable[_]]) {

    def processRequest(req: Request[AnyContent]): Either[Set[String], Witness] = {
        val formVars = req.body.asFormUrlEncoded.get
        var subst = Witness()
        var errorMessages = Set[String]()
        variables.foreach (variable => {
            formVars.get(variable.name) match {
                case Some(formValSeq) =>
                    if (formValSeq.size > 1) {
                        errorMessages += s"Too many values for variable ${variable.name} "
                    } else {
                        val formVal = formValSeq.head.trim
                        if (formVal.length == 0) {
                            errorMessages += s" Empty input for variable ${variable.name} "
                        } else {
                            /// XXX TODO 
                            /// handle typed parameters ...
                            ///variable.clazz match {
                            ///case  classOf[String] =>
                            subst = subst + VariableAssignment(variable.name, formVal)

                            ////}
                        }
                    }

                case None =>
                    errorMessages += s"Missing variable ${variable.name} in form."
            }
        })

        if (errorMessages.size > 0)
            return Left(errorMessages)
        else
            return Right(subst)
    }

}