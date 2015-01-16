package harmony.java.net;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import satisfaction.hadoop.hdfs.CacheJarURLStreamHandlerFactory;
 

/***
 *  Isolate certain classes from classes which may have been previously loaded,
 *   by searching the specified URLS of the classloader before delegating to the parent
 *   ( which is opposite of the standard approach )
 *  
 *   Actually delegate to a hidden inner classloader,
 *    to try to avoid classloader leaks, 
 *     so that 
 *
 */
public class IsolatedClassLoader extends ClassLoader implements java.io.Closeable {
	private final static Logger LOG = LoggerFactory.getLogger( IsolatedClassLoader.class);
	private InnerIsolatedClassLoader _delegLoader = null;

	 
	/**
	public IsolatedClassLoader(URL[] urls, ClassLoader parent) {
		_delegLoader = new InnerIsolatedClassLoader( urls, parent);
	}
	**/
	public IsolatedClassLoader(URL[] urls, ClassLoader parent, List<String> frontLoadedClassExprs, List<String> backLoadedClassExprs, 
			 HiveConf configuration, String cachePath) {
		HiveConf newConf = new HiveConf( configuration);
		_delegLoader = new InnerIsolatedClassLoader( urls, parent, frontLoadedClassExprs, backLoadedClassExprs, newConf,cachePath, this);
        LOG.info(" Creating IsolatedClassLoader " + this + " ; DelegLoader =  " + _delegLoader);
		System.out.println(" Creating IsolatedClassLoader " + this + " ; DelegLoader =  " + _delegLoader);
	}
	
	public void registerClass( Class<?> clazz) {
		try {
	      _delegLoader.registerClass( clazz);
	    } catch( RuntimeException unexpected ) {
	    	unexpected.printStackTrace();
	    	throw unexpected;
	    }
	}
	
	/**
	 * Return the maximum number of times that findClass is retried
	 *   before throwing a ClassNotFoundException, to avoid errors
	 *   caused by transient network conditions.
	 *   
	 * @return
	 */
	public int getMaxRetries() {
	   return _delegLoader.getMaxRetries();	
	}
	
	public void setMaxRetries( int maxRetries ) { 
		_delegLoader.setMaxRetries( maxRetries);
	}
	
	public void addFrontLoadExpr( String expr) {
		try {
		  _delegLoader.addFrontLoadExpr(expr);
	    } catch( RuntimeException unexpected ) {
	    	unexpected.printStackTrace();
	    	throw unexpected;
	    }
	}

	public void addBackLoadExpr( String expr) {
		try {
		   _delegLoader.addBackLoadExpr(expr);
	    } catch( RuntimeException unexpected ) {
	    	unexpected.printStackTrace();
	    	throw unexpected;
	    }
	}


	@Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
		try {
		   return _delegLoader.findClass( name );
	    } catch( RuntimeException unexpected ) {
	    	unexpected.printStackTrace();
	    	throw unexpected;
	    }
	}

	@Override
    protected URL findResource(String name) {
		try {
		  return _delegLoader.findResource( name );
	    } catch( RuntimeException unexpected ) {
	    	unexpected.printStackTrace();
	    	throw unexpected;
	    }
	}
	
	public void release() throws IOException {
		  LOG.info(" Releasing InnerIsolatedClassLoader");
		  _delegLoader = null;
		  System.gc();
	}
	
	@Override
	public void close() throws IOException {
		try {
		  LOG.info(" Closing IsolatedClassLoader " + this);
		  System.out.println(" Closing IsolatedClassLoader " + this);
		  if(_delegLoader != null)
		     _delegLoader.close();
		  
		  //// Need to call clearCache on ReflectionUtils 
		  ////   to avoid classloader leak
		  //// But unfortunately that method is not accessible :(
		  Class reflectUtilClass = Class.forName("org.apache.hadoop.util.ReflectionUtils");
		  Method clearMethod = reflectUtilClass.getDeclaredMethod( "clearCache");
		  clearMethod.setAccessible(true);
		  
		  clearMethod.invoke( null);
		  
		  release();

	    } catch( RuntimeException unexpected ) {
	    	unexpected.printStackTrace();
	    	throw unexpected;
	    } catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			release();
		}
	}
	
	/**
	 *   Delegate the classloading, to find the class before 
	 */
	@Override
	protected Class<?> loadClass(String name, boolean resolve)
		        throws ClassNotFoundException {
	    try {
		    return _delegLoader.loadClass( name, resolve);
	    } catch( RuntimeException unexpected ) {
	    	unexpected.printStackTrace();
	    	throw unexpected;
	    }
    }
	
	    
}
