package harmony.java.net;

import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 

/***
 *  Always lo
 * @author jbanks
 *
 */
public class ReverseClassLoader extends java.net.URLClassLoader {
	private final static Logger LOG = LoggerFactory.getLogger( ReverseClassLoader.class);
	 
	public ReverseClassLoader(URL[] urls, ClassLoader parent) {
		super(urls, parent);
		LOG.info(" Creating RobustClassLoader with URLS " + urls);
	}

	@Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
		int cnt=0;
		while(true) {
		  try {
			  LOG.info(" FINDING CLASS " + name);
		     return super.findClass( name);
		  } catch( ClassNotFoundException notFound) {
			  LOG.warn(" Could not find class "+  name + " after " + cnt +  " tries ; " + notFound.getLocalizedMessage());
		     if( cnt++ >= 10 ) {
		    	 notFound.printStackTrace(System.out);
		    	 LOG.error(" Error while finding class " + name , notFound);
		    	 if(notFound.getCause() != null) {
		    		 notFound.getCause().printStackTrace();
		    		 LOG.error(" Cause of error  " , notFound.getCause());
		    	 }
		    	 throw notFound;
		     }
		  }
		}
	}
	
	/**
	 *  Should we check our URLs to load this class
	 *   before delegating to parent class 
	 * @param name
	 * @return
	 */
	protected boolean shouldFrontLoad( String name ) {
	   return name.contains("SessionState") || name.contains("HiveLocalDriver");
	}

	/**
	 *   Reverse the classloading, to find the class before 
	 */
	@Override
	   protected Class<?> loadClass(String name, boolean resolve)
		        throws ClassNotFoundException
	    {
		   LOG.info(" LOADING CLASS " + name);
		   if( shouldFrontLoad(name)) {
			   return reverseLoadClass(name,resolve);
		   } else {
			   return super.loadClass(name, resolve);
		   }
	    }
	
	    
	   protected Class<?> reverseLoadClass(String name, boolean resolve)
		        throws ClassNotFoundException
	    {
		   LOG.info(" Front loading class " + name);
		            // First, check if the class has already been loaded
		            Class c = findLoadedClass(name);
		            if (c == null) {
		            	ClassNotFoundException rootExc = null;
		            	try {
		                    c = findClass(name);

		                } catch (ClassNotFoundException e) {
		                	rootExc = e;
		                }

		            	if( c == null) {
		                   try {	
		                	   LOG.info(" Going to parent loader for class " + name);
		                     c = getParent().loadClass(name);
		                   } catch( ClassNotFoundException notFoundParent ) {
		                      throw rootExc;
		                   }
		            	}
		            }
		            if (resolve) {
		                resolveClass(c);
		            }
		            return c;
	   }
}
