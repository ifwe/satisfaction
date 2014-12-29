package satisfaction.hadoop.hive

/**
 *   Wrap an object , and access it only through reflection
 *     so that it's class doesn't get loaded in a different
 *      ClassLoader
 */
class Wrapper( val wrapped : Object) extends Object {
     val wrappedClass = wrapped.getClass
   
    /**
     *  Execute a method 
     */
    def ->( methodName  : String, args : Object* ) : Any = {
      /// find the method with the name and specified arguments 
      val method = wrappedClass.getDeclaredMethod( methodName, args.map( _.getClass ):_*)
      method.invoke( wrapped, args : _*)
    }
    
    def execWithParams( methodName  : String, paramTypes: Array[Class[_]], args :Array[_<:Object] ) : Any = {
      /// find the method with the name and specified arguments 
      val method = wrappedClass.getDeclaredMethod( methodName, paramTypes:_*)
      method.invoke( wrapped, args : _*)
    }
    
    /**
     *  Access a public field on the object 
     */
    def ##( fieldName : String) : Any  = {
       val field = wrappedClass.getDeclaredField(fieldName) 
       field.get(wrapped)
    }
    
    def execStatic( methodName : String , args : Object* )  : Any = {
      val method = wrappedClass.getDeclaredMethod( methodName, args.map( _.getClass ):_*)
      method.invoke( null, args : _* )
    }

    /**
     * Update a field
     */
    def ##=( fieldName : String , replaceFieldValue : Any ) : Wrapper = {
       val field = wrappedClass.getDeclaredField(fieldName) 
       field.set(wrapped, replaceFieldValue)
       this
    }
}

object Wrapper {
    def apply( className : String , loader : ClassLoader ) : Wrapper = {
      val clazz : Class[_<: Object] = loader.loadClass(className).asInstanceOf[Class[_<:Object]]
      new Wrapper(clazz.newInstance)
    }
  
    def apply( className : String , loader : ClassLoader, args: Object* ) : Wrapper = {
      val clazz : Class[_<: Object] = loader.loadClass(className).asInstanceOf[Class[_<:Object]]
      val constructor = clazz.getDeclaredConstructor( args.map( _.getClass):_* )
      new Wrapper( constructor.newInstance( args:_*))
    }

    def withConstructor( className : String , loader : ClassLoader, argClasses: Array[Class[_]], args: Array[Object] ) : Wrapper = {
      val clazz : Class[_<: Object] = loader.loadClass(className).asInstanceOf[Class[_<:Object]]
      val constructor = clazz.getDeclaredConstructor( argClasses:_* )
      new Wrapper( constructor.newInstance( args:_* ))
    }

    def execStatic( className : String, loader : ClassLoader,  methodName : String , args : Object* )  : Object = {
      val clazz = loader.loadClass(className)
      val method = clazz.getDeclaredMethod( methodName, args.map( _.getClass ):_*)
      method.invoke( null, args: _*)
    }
}


