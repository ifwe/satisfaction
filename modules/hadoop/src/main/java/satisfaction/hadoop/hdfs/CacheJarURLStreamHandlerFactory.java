package satisfaction.hadoop.hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  To avoid contention on HDFS, cache jar files locally
 *   on the local filesystem
 */
public class CacheJarURLStreamHandlerFactory implements URLStreamHandlerFactory {
	private final static Logger LOG = LoggerFactory.getLogger( CacheJarURLStreamHandlerFactory.class);
	private  FsUrlStreamHandlerFactory hdfsHandlerFactory;
	private String cachePathRoot;
    private Path _tempDir;


	public CacheJarURLStreamHandlerFactory( HiveConf conf, String cachePathRoot) {
        hdfsHandlerFactory = new FsUrlStreamHandlerFactory(conf);
        if(cachePathRoot.startsWith("/")) {
          this.cachePathRoot = cachePathRoot;
        } else {
        	this.cachePathRoot = "/" + cachePathRoot;
        }
	}

	@Override
	public URLStreamHandler createURLStreamHandler(String protocol) {
		if(protocol.equals("jar")) {
		    return new CacheJarStreamHandler();	
		} else {
			return hdfsHandlerFactory.createURLStreamHandler(protocol);
		}
	}
	
	
	private class CacheJarURLConnection extends sun.net.www.protocol.jar.JarURLConnection {
		private JarFile jarFile = null;
		private JarEntry jarEntry = null;
		public CacheJarURLConnection(URL url, CacheJarStreamHandler handler)
				throws MalformedURLException, IOException {
			super(url, handler);
		}
		
		@Override 
		public InputStream getInputStream() throws IOException  {
		   try {
		       connect();
		        InputStream result = null;
		       
		         if (getEntryName() == null) {
		            throw new IOException("no entry name specified");
		          } else {
		            if (jarEntry == null) {
		               throw new FileNotFoundException("JAR entry " + getEntryName() +
		                                  " not found in " +
		                                 jarFile.getName());
		           }
		        result = new JarURLInputStream (jarFile.getInputStream(jarEntry));
		        ///result =  (jarFile.getInputStream(jarEntry));
		       }
		         return result;
		   } catch(IOException ioExc) {
			   LOG.error(" IO Exception " + ioExc.getMessage(), ioExc);
			   throw ioExc;
		   } catch(RuntimeException runExc) {
			   LOG.error(" RuntimeException " + runExc.getMessage(), runExc);
			   throw runExc;
		   }
		}

	   class  JarURLInputStream extends java.io.FilterInputStream {
		  JarURLInputStream (InputStream src) {
		     super (src);
		  }
		  
		  @Override
		  public void close () throws IOException {
		      try {
		         super.close();
		      } finally {
		          if (!getUseCaches()) {
		               jarFile.close();
		          }
		     }
		   }
		 }

		@Override 
		public void connect() throws IOException {
		  try {
			LOG.debug(" CONNECTED !!!" );
			if(!connected) {
				URL jarURL = getJarFileURL();
				LOG.debug(" JAR URL IS " + jarURL +  " FILE IS " + jarURL.getFile());
				String[] jarDirs= jarURL.getFile().split("/");
				String jarName = jarDirs[ jarDirs.length -1 ];
				LOG.debug(" PATH = " + jarURL.getFile() + " JAR NMAME = " + jarName);

				File cacheFile = new File( cachePathRoot +  "/" + jarName  );
				if(!cacheFile.exists()) {
					URLConnection hdfsConnection = jarURL.openConnection(); 
					LOG.debug(" HDFS CONNECTION = "  + hdfsConnection);

					//// Copy to a tempfile and then move in one step
					////  so that nobody reads partial files
					hdfsConnection.setUseCaches(true);
					hdfsConnection.connect();
					InputStream hdfsStream = hdfsConnection.getInputStream();
					///System.out.println(fileURL.getFile() + " HDFS HAS AVAILABLE " + hdfsStream.available());


					Path tempPath= Files.createTempFile(tempDir(), "satisfaction", ".jar" );
					Files.deleteIfExists(tempPath);
					Files.copy( hdfsStream, tempPath );

					hdfsStream.close();

					Path cachedPath = cacheFile.toPath();
					if( !Files.exists( cachedPath.getParent())){
					   Files.createDirectories( cachedPath.getParent());
					}
					Files.copy( tempPath, cacheFile.toPath());
					
				}
			    jarFile =  new JarFile( cacheFile);
			    
			    
			    if ((getEntryName() != null)) {
			    	   jarEntry = (JarEntry)jarFile.getEntry(getEntryName());
			    	   if (jarEntry == null) {
			    	       try {
			    	         if (!getUseCaches()) {
			    	                jarFile.close();
			    	         }
			    	    } catch (Exception e) {
			    	    }
			    	   throw new FileNotFoundException("JAR entry " + getEntryName() +
			    	                     " not found in " +
			    	                      jarFile.getName());
			    	   }
			    }
			    connected = true;
			}
		  } catch(FileNotFoundException notExc ) {
				//// Swallow these
				throw notExc;
		 } catch(IOException ioExc ) {
				LOG.error("IO Exception " + ioExc.getMessage(), ioExc );
				throw ioExc;
		 } catch(Throwable unexpected ) {
				LOG.error(" Unexpected error " + unexpected.getMessage(), unexpected );
				throw new RuntimeException(unexpected);
			}
		}
		
		@Override
		public JarFile getJarFile() throws IOException {
			connect();
		   return jarFile;	
		}
	}
	
	
	private class CacheJarStreamHandler extends sun.net.www.protocol.jar.Handler {

		@Override
		protected URLConnection openConnection(URL jarURL) throws IOException {
			return new CacheJarURLConnection(jarURL, this); 
		}

	}
	
	
	protected Path tempDir() throws IOException {
		if(_tempDir == null) {
		   Path currentDir = FileSystems.getDefault().getPath(cachePathRoot);
		   _tempDir = Files.createTempDirectory(currentDir, "satis_cache");
		}
		return _tempDir;
	}


}
