package harmony.java.net;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import satisfaction.hadoop.hdfs.CachingURLStreamHandlerFactory;
 

/***
 *  Isolate certain classes from classes which may have been previously loaded,
 *   by searching the specified URLS of the classloader before delegating to the parent
 *   ( which is opposite of the standard approach )
 *  
 *
 */
public class IsolatedClassLoader extends java.net.URLClassLoader {
	private final static Logger LOG = LoggerFactory.getLogger( IsolatedClassLoader.class);
	private List<Pattern> frontLoadPatterns;
	private List<Pattern> backLoadPatterns;
	private Map<String,Class<?>> registeredClasses = new HashMap<String,Class<?>>();
	private int maxRetries = 10;

	 
	public IsolatedClassLoader(URL[] urls, ClassLoader parent) {
		super(urls, parent);
		LOG.info(" Creating RobustClassLoader with URLS " + urls);
		frontLoadPatterns = new ArrayList<Pattern>();
		backLoadPatterns = new ArrayList<Pattern>();
	}
	public IsolatedClassLoader(URL[] urls, ClassLoader parent, List<String> frontLoadedClassExprs, List<String> backLoadedClassExprs, 
			 HiveConf configuration, String cachePath) {
		super(urls, parent, new CachingURLStreamHandlerFactory( configuration, cachePath));
		configuration.setBoolean("fs.hdfs.impl.disable.cache", false); /// ???
		LOG.info(" Creating RobustClassLoader with URLS " + urls);
		frontLoadPatterns = new ArrayList<Pattern>();
	    for( String expr : frontLoadedClassExprs ) {
           LOG.info(" Adding Pattern " + expr + " to frontloaded patterns");
	       frontLoadPatterns.add( Pattern.compile( expr));
	    }
		
		backLoadPatterns = new ArrayList<Pattern>();
	    for( String expr : backLoadedClassExprs ) {
           LOG.info(" Adding Pattern " + expr + " to backloaded patterns");
	       backLoadPatterns.add( Pattern.compile( expr));
	    }
		
	}
	
	public void registerClass( Class<?> clazz) {
	   registeredClasses.put( clazz.getName(), clazz);
	}
	
	/**
	 * Return the maximum number of times that findClass is retried
	 *   before throwing a ClassNotFoundException, to avoid errors
	 *   caused by transient network conditions.
	 *   
	 * @return
	 */
	public int getMaxRetries() {
	   return maxRetries;	
	}
	
	public void setMaxRetries( int maxRetries ) { 
		this.maxRetries = maxRetries;
	}
	
	public void addFrontLoadExpr( String expr) {
		this.frontLoadPatterns.add(Pattern.compile(expr) );
	}

	public void addBackLoadExpr( String expr) {
		this.backLoadPatterns.add(Pattern.compile(expr) );
	}


	@Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
		int cnt=0;
		while(true) {
		  try {
			  LOG.debug(" Finding " + name);
		     return super.findClass( name);
		  } catch( ClassNotFoundException notFound) {
			  LOG.warn(" Could not find class "+  name + " after " + cnt +  " tries ; " + notFound.getLocalizedMessage());
		     if( cnt++ >= 10 ) {
		    	 if(notFound.getCause() != null) {
		    		 notFound.getCause().printStackTrace();
		    		 LOG.error(" Cause of error  " , notFound.getCause());
		    	 }
		    	 throw notFound;
		     }
		  } catch( IllegalStateException badState) {
			  LOG.error(" Bad State Exception while finding Class " + name + " message is " + badState.getMessage() , badState);
		  }
		}
	}
	
	/**
	 *  Should we load this class from our specified set of URLs
	 *   before delegating to parent class 
	 *  
	 *   Current implementation is check the front load Expressions, 
	 *    and see if the class name matches the expression.
	 *   If it does, it returns true, unless the string also matches
	 *    the backloaded expressions, which then returns false, and 
	 *     the class is loaded from the parent loader.
	 *   
	 * @param name
	 * @return
	 */
	protected boolean shouldFrontLoad( String name ) {
		boolean frontFlag = false;
		for( Pattern frontPattern : frontLoadPatterns) {
			Matcher m = frontPattern.matcher( name);
			if(m.matches() ) {
				LOG.info( " Class " + name + " matches frontload pattern " + frontPattern.pattern());
				frontFlag = true;
				break;
			}
		}
		if( frontFlag) {
		   for( Pattern backPattern : backLoadPatterns) {
			  Matcher m = backPattern.matcher( name);
			  if( m.matches()) {
				  LOG.info( " Class " + name + " matches backload pattern " + backPattern.pattern() + " ; Delegating to Parent ");
				  return false;
			  }
		   }
		   return true;
		} else {
			return false;
		}
	}

	/**
	 *   Reverse the classloading, to find the class before 
	 */
	@Override
	   protected Class<?> loadClass(String name, boolean resolve)
		        throws ClassNotFoundException
	    {
		   LOG.debug(" Loading class " + name);
		   if( registeredClasses.containsKey( name)) {
			  return registeredClasses.get(name);
		   }
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
