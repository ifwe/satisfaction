package satisfaction
package util

/**
 *   Hold a reference behind a handle so that reference can be more easily released
 */
class Releaseable[T>:Null]( factory :  => T , finalizerFunc : ( T => Unit)  ) {
    private var _inner : T = null
   
    def isBuilt : Boolean = { _inner != null }
    def get : T = {
       if( _inner == null )  {
          _inner = factory
       }
       _inner
    }
    
    def release = {
      System.out.println(" Please RELEASE ME , LET ME GO !!!" )
      if(_inner != null) {
         finalizerFunc( _inner)
         _inner =  null
       }
       System.gc 
    }

}