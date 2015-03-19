package satisfaction.util.classloader;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.HiveParser.keyProperty_return;
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
public class InnerIsolatedClassLoader extends java.net.URLClassLoader implements java.io.Closeable {
	private final static Logger LOG = LoggerFactory.getLogger( InnerIsolatedClassLoader.class);
	private List<Pattern> frontLoadPatterns;
	private List<Pattern> backLoadPatterns;
	private int maxRetries = 10;
	private IsolatedClassLoader outerLoader;
    private Map<String,Class> registeredClasses = new HashMap<String,Class>();
	private static boolean _isClosing = false;



	 
	public InnerIsolatedClassLoader(URL[] urls, ClassLoader parent, List<String> frontLoadedClassExprs, List<String> backLoadedClassExprs, 
			 HiveConf configuration, String cachePath, IsolatedClassLoader outerLoader) {
		super(urls, parent, new CacheJarURLStreamHandlerFactory( configuration, cachePath));
		LOG.info(" Creating InnerIsolatedClassLoader with URLS " + urls);
		System.out.println(" Creating InnerIsolatedClassLoader with URLS " + urls);
		frontLoadPatterns = new ArrayList<Pattern>();
	    for( String expr : frontLoadedClassExprs ) {
           LOG.info(" Adding Pattern " + expr + " to frontloaded patterns");
           System.out.println(" Adding Pattern " + expr + " to frontloaded patterns");
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
	
	
	public IsolatedClassLoader getOuterLoader() {
		return outerLoader;
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
			  if( registeredClasses.containsKey(name)) {
				  return registeredClasses.get(name);
			  }
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
	
	@Override
	public void close() throws IOException {
		if(_isClosing == true ) {
			LOG.info(" Sorry, we're already closed ");
			System.out.println(" Sorry, we're already closed ");
			if(this.outerLoader != null) {
			  this.outerLoader = null;	
			}
			return;
		} else {
		     LOG.info(" Closing InnerIsolatedClassLoader " + this);
		     System.out.println(" Closing InnerIsolatedClassLoader " + this);
			_isClosing = true;
		}
      try {
		LOG.info(" Closing InnerIsolatedClassLoader " + this);
		registeredClasses.clear();
		
		  
		LOG.info(" Ready to remove any Static Cache References");
		System.out.println(" Ready to remove any Static Cache References");
		
		removeStaticCacheReference("org.apache.hadoop.io.WritableComparator");
		removeStaticCacheReference("org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory");
		removeStaticCacheReference("org.apache.thrift.meta_data.FieldMetaData");
		removeStaticCacheReference("com.tagged.hadoop.hive.serde2.avro.SchemaFactory");
		removeStaticCacheReference("org.apache.hadoop.io.compress.CompressionCodecFactory");
		removeStaticCacheReference("org.apache.hadoop.mapreduce.Cluster");
		
		removeReflectAsmReferences();
		
		removeShutdownReferences();
		
		clearCacheOnReflectionUtils();
		
		LOG.info(" Static cache References should be removed ");
		System.out.println(" Static cache References should be removed ");
		  
		if(outerLoader != null) {
	      LOG.info(" Releasing OuterLoader " + outerLoader);
	      System.out.println(" Releasing OuterLoader " + outerLoader);
		  outerLoader.release();
		  outerLoader = null;
		} else {
		  LOG.warn(" Close called multiple times on InnerIsolatedClassLoader " + this);
		  System.out.println(" Close called multiple times on InnerIsolatedClassLoader " + this);
		}
		
		
      } finally {
        System.out.println(" Calling super.close");
		super.close();
		////System.out.println(" Clearing out loaded classes ");
		///removeLoadedClasses();
      }
	}

	private HashSet<Object> _seenObjs = new HashSet();
	protected void removeStaticCacheReference(String className ) {
		removeStaticCacheReference(this,className);
		removeStaticCacheReference(getParent(),className);
		removeStaticCacheReference(this.getClass().getClassLoader(),className);
		removeStaticCacheReference(outerLoader.getClass().getClassLoader(),className);
	}
		
		
	protected void removeStaticCacheReference(ClassLoader loader, String className ) {
		try {
			_seenObjs.clear();
			Class staticCacheClass = loader.loadClass(className);
			////Class staticCacheClass = getParent().loadClass(className);
			LOG.info(" Removing static cached references for class " + className + " :: " + staticCacheClass + " :: LOADER " + staticCacheClass.getClassLoader());
			System.out.println(" Removing static cached references for class " + className + " :: " + staticCacheClass + " :: LOADER " + staticCacheClass.getClassLoader());

			Field[] fields = staticCacheClass.getDeclaredFields();
			for(Field field : fields) {
				//// For top level, only check static fields
				if( Modifier.isStatic( field.getModifiers())) {
					System.out.println(" Checking field " + className + "." + field.getName());
					recursiveRemoveCachedFieldReference( null,field);
			     }
			}
		} catch(Exception exc ) {
			LOG.error(" Unexpected error trying to remove Cached static reference from class " + className+ " :: " +exc.getMessage() , exc );
			System.out.println(" Unexpected error trying to remove Cached static reference from class " + className+ " :: " +exc.getMessage() );
			exc.printStackTrace();
		} finally {
		    _seenObjs.clear();	
		}
	}
	
	private void recursiveRemoveCachedFieldReference(Object parentObj, Field field) {
		try {
			field.setAccessible(true);
			Object fieldObj = field.get( parentObj);
			System.out.println(" Object " + fieldObj + " of " + field + " of " + parentObj  );
			if( fieldObj != null) {
              System.out.println("  Classloader of FieldObj is " + fieldObj.getClass().getClassLoader() );
			  if( _seenObjs.contains(fieldObj)) {
			    System.out.println(" Already saw Reference " + fieldObj + " on field " + field + " ; skipping ");
			    return;
			  } else {
			    _seenObjs.add( fieldObj);
			  }
              if(java.util.Map.class.isAssignableFrom(field.getType()) )  {
			    recursiveRemoveCachedMapReference( (Map) fieldObj);
		      } else if( java.util.List.class.isAssignableFrom(field.getType())) {
			    recursiveRemoveCachedListReference( (List) fieldObj);
		      } else {
			    ///// Forcibly set to null if there for some reason 
			    if(fieldObj != null) {
		          if(fieldObj.getClass().getClassLoader() == this
			        || fieldObj.getClass().getClassLoader() == outerLoader
		    	    || fieldObj == this
		    	    || fieldObj == outerLoader
		    	    || ((fieldObj instanceof Class) && ((Class)fieldObj).getClassLoader() == this)
		    	    || ((fieldObj instanceof Class) && ((Class)fieldObj).getClassLoader() == outerLoader)) {
			          LOG.info(" Forcibly setting to null field " + field.getName() + " on class " + field.getDeclaringClass().getName());
				      System.out.println(" Forcibly setting to null field " + field.getName() + " on class " + field.getDeclaringClass().getName());
			          field.set(fieldObj, null);
		         } else {
			      //// Otherwise 
		    	  LOG.info(" Recursively checking object " + fieldObj + " for references to inner classLoader" );
		    	  System.out.println(" Recursively checking object " + fieldObj + " for references to inner classLoader" );
		    	  Field[] childFields =fieldObj.getClass().getDeclaredFields();
		    	  for(Field childField : childFields) {
		    		//// For other fields, only check non static fields
		    		if( !Modifier.isStatic( childField.getModifiers())) {
		    	     System.out.println(" Checking Field " + childField.getName());
		    	     recursiveRemoveCachedFieldReference( fieldObj, childField);
		    		}
		    	  }
			   }
			 }
		  }
		}
      }  catch(Exception unexpected) {
    	  LOG.error(" Problem while recursively looking for referenced fields " + unexpected.getMessage(), unexpected);
    	  System.out.println(" Problem while recursively looking for referenced fields " + unexpected.getMessage() );
    	  unexpected.printStackTrace();
      } 
	}
		
	private void recursiveRemoveCachedMapReference(Map valMap) {
		if( _seenObjs.contains( valMap)) {
			System.out.println(" Already saw Map  ; skipping ");
			return;
		} else {
			_seenObjs.add(valMap);
		}
		LOG.info(" Recursively checking value Map " + valMap
				+ " for references");
		System.out.println(" Recursively checking value Map  for references");
		List removeList = new ArrayList();
		for (Object valKey : valMap.keySet()) {
			Object val = valMap.get(valKey);
			if (val != null) {
				if (val.getClass().getClassLoader() == this
						|| valKey.getClass().getClassLoader() == this
						|| val.getClass().getClassLoader() == outerLoader
						|| valKey.getClass().getClassLoader() == outerLoader
						|| val == this
						|| valKey == this
		    	        || ((val instanceof Class) && ((Class)val).getClassLoader() == this)
		    	        || ((val instanceof Class) && ((Class)val).getClassLoader() == outerLoader)
		    	        || ((valKey instanceof Class) && ((Class)valKey).getClassLoader() == this)
		    	        || ((valKey instanceof Class) && ((Class)valKey).getClassLoader() == outerLoader)
						) {
					removeList.add(valKey);
				} else {
					//// Thread check ???
					if (val instanceof Thread) {
						Thread thr = (Thread) val;
						if (thr.getContextClassLoader() == this
								|| thr.getContextClassLoader() == outerLoader) {
							LOG.info(" Removing thread with access class loader ");
							System.out.println(" Removing thread with access class loader ");
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
		    		       if( !Modifier.isStatic( checkField.getModifiers())) {
							recursiveRemoveCachedFieldReference( val, checkField);
		    		       }
						}
						Field[] keyFields = valKey.getClass().getDeclaredFields();
						for(Field keyField : keyFields) {
		    		       if( !Modifier.isStatic( keyField.getModifiers())) {
							recursiveRemoveCachedFieldReference( valKey, keyField);
		    		       }
						}
					}
				}
			}
		}
		for (Object removeObj : removeList) {
			LOG.info(" Removing cached reference " + removeObj + " from map "
					+ valMap);
			System.out.println(" Removing cached reference " + removeObj + " from map "
					+ valMap);
			valMap.remove(removeObj);
		}
	}
	
	
	private void recursiveRemoveCachedListReference( List valList ) {
		if( _seenObjs.contains( valList)) {
			System.out.println(" Already saw List ; skipping ");
			return;
		} else {
			_seenObjs.add( valList);
		}
		LOG.info(" Recursively checking value List " + valList + " for references");
		System.out.println(" Recursively checking value List " + valList + " for references");
		List removeList = new ArrayList();
		for( Object val : valList) {
			if(val != null) {
				if( val == this 
						|| val == outerLoader
						|| val.getClass().getClassLoader() == this 
						|| val.getClass().getClassLoader() == outerLoader
		    	        || ((val instanceof Class) && ((Class)val).getClassLoader() == this)
		    	        || ((val instanceof Class) && ((Class)val).getClassLoader() == outerLoader) ) {
					removeList.add( val );
				} else {
					if( val instanceof Thread ) {
						Thread thr = (Thread)val;
						if(thr.getContextClassLoader() == this
								|| thr.getContextClassLoader() == outerLoader) {
							LOG.info(" Removing thread with access class loader ");
							System.out.println(" Removing thread with access class loader ");
							removeList.add(val);
						}
					} else if( List.class.isAssignableFrom( val.getClass() )) {
						recursiveRemoveCachedListReference( (List)val);
					} else if( Map.class.isAssignableFrom( val.getClass() )) {
						recursiveRemoveCachedMapReference( (Map)val);
					} else {	
						Field[] checkFields = val.getClass().getDeclaredFields();
						for( Field checkField : checkFields) {
						   recursiveRemoveCachedFieldReference(val, checkField);
						}
					}
				}
			}
		}
		for(Object removeObj : removeList) {
			LOG.info(" Removing cached reference " + removeObj + " from list " + valList );
			System.out.println(" Removing cached reference " + removeObj + " from list " + valList );
			valList.remove( removeObj );
		}
	}
	
	protected void removeLoadedClasses() {
	  try { 
		Class parentClass = this.getClass().getSuperclass();
		Field[] parentFields = parentClass.getDeclaredFields();
		System.out.println(" Parent Class is " + parentClass.getName());
		for(Field parentField : parentFields) {
			System.out.println("   PArent field =  " + parentField.getName());
		}
		Class grandParentClass = parentClass.getSuperclass();
		System.out.println(" GrandParent Class is " + grandParentClass.getName());
		Field[] grandParentFields = grandParentClass.getDeclaredFields();
		for(Field grandParentField : grandParentFields) {
			System.out.println("  Grand  PArent field =  " + grandParentField.getName());
		}
		Class grGrandParentClass = grandParentClass.getSuperclass();
		System.out.println(" Great GrandParent Class is " + grGrandParentClass.getName());
		Field[] grGrandParentFields = grGrandParentClass.getDeclaredFields();
		for(Field grGrandParentField : grGrandParentFields) {
			System.out.println("  Great Grand  PArent field =  " + grGrandParentField.getName());
		}

	    Field classesField = grGrandParentClass.getDeclaredField("classes");
		 classesField.setAccessible(true);
			  
			  Vector parentClasses = (Vector)classesField.get( this);
			  System.out.println( "Number of loaded classes  " + parentClasses.size());
			  parentClasses.clear();

			} catch(Throwable unexpected) {
			  System.out.println("Unexpected error while trying to clear out parent classes field");
			  unexpected.printStackTrace();
			}
		
	}
	
	protected void clearCacheOnReflectionUtils() {
	  //// Need to call clearCache on ReflectionUtils 
	  ////   to avoid classloader leak
	  //// But unfortunately that method is not accessible :(
	  LOG.info(" Calling method clearCache on ReflectionUtils ");
	  System.out.println(" Calling method clearCache on ReflectionUtils ");
	  try {
	    Class reflectUtilClass = Class.forName("org.apache.hadoop.util.ReflectionUtils");
	    ////Class reflectUtilClass = getParent().loadClass("org.apache.hadoop.util.ReflectionUtils");
	    Method clearMethod = reflectUtilClass.getDeclaredMethod( "clearCache");
	    clearMethod.setAccessible(true);
	  
	    clearMethod.invoke( null);
	  } catch (Exception e) {
		LOG.error("Unexpected error while trying to call ClearCache on ReflectionUtils  ; " + e.getMessage(), e);
		System.out.println("Unexpected error while trying to call ClearCache on ReflectionUtils  ; " + e.getMessage());
		e.printStackTrace();
	  }
    }

	
	protected void removeShutdownReferences() {
		try {
			removeStaticCacheReference("java.lang.ApplicationShutdownHooks");
			
		  ////Class shutdownHooksClass = this.getClass().getClassLoader().loadClass( "java.lang.ApplicationShutdownHooks");
		} catch (Exception e) {
			LOG.error("Unexpected error while trying to remove references to shutdown hooks ; " + e.getMessage(), e);
			System.out.println("Unexpected error while trying to remove references to shutdown hooks ; " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	/**
	 * Static reference in some versions of ReflectAsm
	 *   hold onto the parent class in AccessClassLoader
	 *   
	 */
	protected void removeReflectAsmReferences() {
		try {
	      LOG.info(" Removing any reference to ReflectAsm AccessClassLoader ");
	      System.out.println(" Removing any reference to ReflectAsm AccessClassLoader ");
		  Class reflectAsmClass = this.getClass().getClassLoader().loadClass( "org.apache.hive.com.esotericsoftware.reflectasm.AccessClassLoader");
		  Field accessClassLoadersField = reflectAsmClass.getDeclaredField("accessClassLoaders");
		  accessClassLoadersField.setAccessible(true);
		  
		  Method getParentMethod = reflectAsmClass.getSuperclass().getDeclaredMethod("getParent");
		  getParentMethod.setAccessible(true);
		  
		  List accessClassLoaders  =  (List) accessClassLoadersField.get( null);
		  for(Object accessClassLoader : accessClassLoaders ) {
			 Object parent = getParentMethod.invoke( accessClassLoader);
			 System.out.println( " AccessClassLoader = " + accessClassLoader + " Parent is " + parent);
			 if( parent == this || parent == this.outerLoader) {
				 LOG.info(" Removing AccessClassLoader " + accessClassLoader +  " from static list accessClassLoaders ");
				 System.out.println(" Removing AccessClassLoader " + accessClassLoader +  " from static list accessClassLoaders ");
				 accessClassLoaders.remove( accessClassLoader);
			 }
		  }

		} catch( Exception unexpected ) {
		  LOG.error(" Unexpected error while trying to remove reference to AccessClassLoaders ", unexpected);	
		  System.out.println(" Unexpected error while trying to remove reference to AccessClassLoaders " + unexpected.getMessage());	
		  unexpected.printStackTrace();
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
		   if( registeredClasses.containsKey(name)) {
			   return registeredClasses.get(name);
		   }
		   if( shouldFrontLoad(name)) {
			   return reverseLoadClass(name,resolve);
		   } else {
			   ////return parentLoader.loadClass(name);
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
