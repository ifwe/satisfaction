package satisfaction.util.classloader;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import satisfaction.hadoop.hdfs.CacheJarURLStreamHandlerFactory;
 

/***
 *  Isolate certain classes from classes which may have been previously loaded,
 *   by searching the specified URLS of the classloader before delegating to the parent
 *   ( which is opposite of the standard approach )
 *  
 *
 */
class InnerIsolatedClassLoader extends java.net.URLClassLoader implements java.io.Closeable {
	private final static Logger LOG = LoggerFactory.getLogger( InnerIsolatedClassLoader.class);
	private List<Pattern> frontLoadPatterns;
	private List<Pattern> backLoadPatterns;
	private Map<String,Class<?>> registeredClasses = new HashMap<String,Class<?>>();
	private int maxRetries = 10;
	private IsolatedClassLoader outerLoader;

	 
	public InnerIsolatedClassLoader(URL[] urls, ClassLoader parent, List<String> frontLoadedClassExprs, List<String> backLoadedClassExprs, 
			 HiveConf configuration, String cachePath, IsolatedClassLoader outerLoader) {
		super(urls, parent, new CacheJarURLStreamHandlerFactory( configuration, cachePath));
		configuration.setBoolean("fs.hdfs.impl.disable.cache", false); /// ???
		LOG.info(" Creating InnerIsolatedClassLoader with URLS " + urls);
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
		
	    this.outerLoader = outerLoader;
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
	 * Copy a class from the parentloader, and 
	 *  make it seem like it was loaded from this class
	 * @param name
	 * @return
	 * @throws ClassNotFoundException
	 */
	protected Class<?> copyParentClass( String className) throws ClassNotFoundException {
		try {
		   String classAsPath = className.replace('.', '/') + ".class";
		    InputStream stream = getParent().getResourceAsStream(classAsPath);
		
			byte[] classBytes = IOUtils.toByteArray(stream);
			
			return defineClass(className, classBytes,0, classBytes.length);
		} catch (IOException e) {
			LOG.error("Error while trying to copy parent class " + className, e);
			throw new ClassNotFoundException("Trouble copyting parent class", e);
		}
		
	}
	
	@Override
	public void close() throws IOException {
		LOG.info(" Closing InnerIsolatedClassLoader " + this);
		registeredClasses.clear();
		super.close();
		
		  
		try {
		  //// Need to call clearCache on ReflectionUtils 
		  ////   to avoid classloader leak
		  //// But unfortunately that method is not accessible :(
		  Class reflectUtilClass = Class.forName("org.apache.hadoop.util.ReflectionUtils");
		  Method clearMethod = reflectUtilClass.getDeclaredMethod( "clearCache");
		  clearMethod.setAccessible(true);
		  
		  clearMethod.invoke( null);
		} catch(Exception exc ) {
			LOG.error(" Unexpected Error while attempting to Clear ReflectionUtils", exc);
		}
		
		
		removeStaticCacheReference("org.apache.hadoop.io.WritableComparator");
		removeStaticCacheReference("org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory");
		
		removeShutdownReferences();
		  
		outerLoader.release();
	}
	
	protected void removeStaticCacheReference(String className ) {
		try {
			Class staticCacheClass = Class.forName(className);
			Field[] fields = staticCacheClass.getDeclaredFields();
			for(Field field : fields) {
				if( Modifier.isStatic( field.getModifiers())) {
			      field.setAccessible(true);
			      Object fieldObj = field.get( null);
			      if(fieldObj != null) {
				    if(java.util.Map.class.isAssignableFrom(field.getType()) )  {
					  recursiveRemoveCachedMapReference( (Map) fieldObj);
				    } else if( java.util.List.class.isAssignableFrom(field.getType())) {
					  recursiveRemoveCachedListReference( (List) fieldObj);
				    } else {
					  ///// Forcibly set to null if there for some reason 
					  if(fieldObj.getClass().getClassLoader() == this) {
						  LOG.info(" Forcibly setting to null field " + field.getName() + " on class " + className);
						  field.set(null, null);
					  }
				    }
			      }
				}
			}
		} catch(Exception exc ) {
			LOG.error(" Unexpected error trying to remove Cached static reference from class " + className+ " :: " +exc.getMessage() , exc );
		}
	}
	
	private void recursiveRemoveCachedMapReference(Map valMap) {
		LOG.info(" Recursively checking value Map " + valMap
				+ " for references");
		List removeList = new ArrayList();
		for (Object valKey : valMap.keySet()) {
			Object val = valMap.get(valKey);
			if (val != null) {
				if (val.getClass().getClassLoader() == this
						|| valKey.getClass().getClassLoader() == this) {
					removeList.add(valKey);
				} else {
					if (val instanceof Thread) {
						Thread thr = (Thread) val;
						if (thr.getContextClassLoader() == this
								|| thr.getContextClassLoader() == outerLoader) {
							LOG.info(" Removing thread with access class loader ");
							removeList.add(valKey);
						}
					} else if (List.class.isAssignableFrom(val.getClass())) {
						recursiveRemoveCachedListReference((List) val);
					} else if (Map.class.isAssignableFrom(val.getClass())) {
						recursiveRemoveCachedMapReference((Map) val);
					} else {
						Field[] checkFields = val.getClass()
								.getDeclaredFields();
						for (Field checkField : checkFields) {
							try {
								checkField.setAccessible(true);
								Object checkObj = checkField.get(val);
								if (checkObj != null) {
									if (checkObj.getClass().getClassLoader() == this) {
										LOG.info(" Adding to remove list field "
												+ checkField.getName()
												+ " value " + checkObj);
										removeList.add(valKey);
									} else {
										if (List.class
												.isAssignableFrom(checkField
														.getType())) {
											List checkListObj = (List) checkObj;
											recursiveRemoveCachedListReference(checkListObj);
										} else if (Map.class
												.isAssignableFrom(checkField
														.getType())) {
											checkField.setAccessible(true);
											Map checkMapObj = (Map) checkObj;
											recursiveRemoveCachedMapReference(checkMapObj);
										}
									}
								}
							} catch (IllegalAccessException ill) {
								LOG.error(" IllegalAccessException while trying to access field "
										+ checkField.getName()
										+ " on object "
										+ val);
							}
						}
					}
				}
			}
		}
		for (Object removeObj : removeList) {
			LOG.info(" Removing cached reference " + removeObj + " from map "
					+ valMap);
			valMap.remove(removeObj);
		}
	}
	
	
	private void recursiveRemoveCachedListReference( List valList ) {
		LOG.info(" Recursively checking value List " + valList + " for references");
		List removeList = new ArrayList();
		for( Object val : valList) {
			if(val != null) {
				if( val.getClass().getClassLoader() == this ) {
					removeList.add( val );
				} else {
					if( val instanceof Thread ) {
						Thread thr = (Thread)val;
						if(thr.getContextClassLoader() == this
								|| thr.getContextClassLoader() == outerLoader) {
							LOG.info(" Removing thread with access class loader ");
							removeList.add(val);
						}
					} else if( List.class.isAssignableFrom( val.getClass() )) {
						recursiveRemoveCachedListReference( (List)val);
					} else if( Map.class.isAssignableFrom( val.getClass() )) {
						recursiveRemoveCachedMapReference( (Map)val);
					} else {	
						Field[] checkFields = val.getClass().getDeclaredFields();
						for( Field checkField : checkFields) {
							try {
								checkField.setAccessible(true);
								Object checkObj = checkField.get( val);
								if(checkObj != null) {
									if(checkObj.getClass().getClassLoader() == this) {
										LOG.info(" Adding to remove list field " + checkField.getName() + " value " + checkObj);
										removeList.add( val);
									} else {
										if( List.class.isAssignableFrom( checkField.getType() ) ) {
											List checkListObj = (List) checkObj;
											recursiveRemoveCachedListReference( checkListObj );
										} else if( Map.class.isAssignableFrom( checkField.getType())) {
											Map checkMapObj = (Map) checkObj;
											recursiveRemoveCachedMapReference( checkMapObj );
										}
									}
								}
							} catch(IllegalAccessException ill) {
								LOG.error(" IllegalAccessException while trying to access field " + checkField.getName() + " on object " + val); 
							}
						}
					}
				}
			}
		}
		for(Object removeObj : removeList) {
			LOG.info(" Removing cached reference " + removeObj + " from list " + valList );
			valList.remove( removeObj );
		}
	}

	
	protected void removeShutdownReferences() {
		try {
			////Class shutdownHookClass = Class.forName( "java.lang.ApplicationShutdownHooks");
			removeStaticCacheReference("java.lang.ApplicationShutdownHooks");
		} catch (Exception e) {
			LOG.error("Unexpected error while trying to remove references to shutdown hooks ; " + e.getMessage(), e);
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
				LOG.debug( " Class " + name + " matches frontload pattern " + frontPattern.pattern());
				frontFlag = true;
				break;
			}
		}
		if( frontFlag) {
		   for( Pattern backPattern : backLoadPatterns) {
			  Matcher m = backPattern.matcher( name);
			  if( m.matches()) {
				  LOG.debug( " Class " + name + " matches backload pattern " + backPattern.pattern() + " ; Delegating to Parent ");
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
		   LOG.debug(" Front loading class " + name);
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
		                	   LOG.debug(" Going to parent loader for class " + name);
		                     c = getParent().loadClass(name);
		                	   ///c = copyParentClass(name);
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
