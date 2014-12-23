package satisfaction.hadoop.hdfs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.jar.JarFile;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 *  To avoid contention on HDFS, cache jar files locally
 *   on the filesystem
 */
public class CachingURLStreamHandlerFactory implements URLStreamHandlerFactory {
	private  FsUrlStreamHandlerFactory hdfsHandlerFactory;
	private  URLStreamHandler fileHandler;
	private  URLStreamHandler hdfsHandler;
	private String cachePathRoot;

	public CachingURLStreamHandlerFactory( HiveConf conf, String cachePathRoot) {
        hdfsHandlerFactory = new FsUrlStreamHandlerFactory(conf);
        if(cachePathRoot.startsWith("/")) {
          this.cachePathRoot = cachePathRoot;
        } else {
        	this.cachePathRoot = "/" + cachePathRoot;
        }
        fileHandler = hdfsHandlerFactory.createURLStreamHandler("file");
        hdfsHandler = hdfsHandlerFactory.createURLStreamHandler("hdfs");
	}

	@Override
	public URLStreamHandler createURLStreamHandler(String protocol) {
		///System.out.println(" CACHING STREAM HANDLER PROTOCOL = " + protocol);
		if(protocol.equals("jar")) {
		    return new CacheingUrlStreamHandler();	
		} else {
			return hdfsHandlerFactory.createURLStreamHandler(protocol);
		}
	}
	
	
	private URLConnection getConnection( URLStreamHandler handler, URL url) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, MalformedURLException {
		if(url.toString().endsWith("!/")) {
		  url = new URL( url.toString().substring( 0, url.toString().length() - 2));
		}
		///System.out.println( " HANDLER GET CONNECTiON " + url);
		Method connectionMethod = handler.getClass().getDeclaredMethod("openConnection", new Class[] { URL.class });
        connectionMethod.setAccessible(true);
        
        Object connObj = connectionMethod.invoke( handler, url);
        return (URLConnection) connObj;
	}
	
	private class CachingJarConnection extends JarURLConnection {
		private URL cachedFileUrl;

		public CachingJarConnection( URL jarURL , URL fileURL) throws MalformedURLException {
			super(jarURL);
			cachedFileUrl = fileURL;
		}

		@Override
		public JarFile getJarFile() throws IOException {
			//// Jar willl be cached locally by now 
			JarFile jarFile = new JarFile( cachedFileUrl.getFile() );
			return jarFile;
		}

		@Override
		public void connect() throws IOException {
		   ///System.out.println(" CONNECT ");
		}
		
	}
	
	private class CacheingUrlStreamHandler extends URLStreamHandler {
        private Path _tempDir;

		@Override
		protected URLConnection openConnection(URL jarURL) throws IOException {
			try {
				////System.out.println(" OPTEN CONNECTION "+ jarURL.getProtocol() +  " file = " + jarURL.getFile());

				URL fileURL = new URL(jarURL.getFile());
				String[] jarDirs= fileURL.getPath().split("/");
				String jarName = jarDirs[ jarDirs.length -1 ];
				////System.out.println(" FILE URL " + fileURL + " FILE = " + fileURL.getFile() + " PATH = " + fileURL.getPath() + " JAR NMAME = " + jarName);
				

				File cacheFile = new File( cachePathRoot +  "/" + jarName  );
				URL mappedURL = new URL("file://" + cachePathRoot + "/" + jarName );
				////System.out.println(" CACHED FILE = " + cacheFile+ " MAPPED URL = "+ mappedURL );
				if(cacheFile.exists()) {
					///System.out.println(" CACHE FILE EXISTS");
					////return getConnection(fileHandler, mappedURL);
					return new CachingJarConnection( jarURL, mappedURL );
				} else {
					///System.out.println(" COPYING JAR FILE ");
					URLConnection hdfsConnection = getConnection(hdfsHandler, fileURL );
					////System.out.println(" HDFS CONNECTION = "  + hdfsConnection);

					//// Copy to a tempfile and then move in one step
					////  so that nobody reads partial files
					hdfsConnection.setUseCaches(true);
					hdfsConnection.connect();
					InputStream hdfsStream = hdfsConnection.getInputStream();
					System.out.println(fileURL.getFile() + " HDFS HAS AVAILABLE " + hdfsStream.available());


					Path tempPath= Files.createTempFile(tempDir(), "satisfaction", ".jar" );
					Files.deleteIfExists(tempPath);
					Files.copy( hdfsStream, tempPath );

					hdfsStream.close();

					////Files.copy( tempPath, cacheFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
					Path cachedPath = cacheFile.toPath();
					if( !Files.exists( cachedPath.getParent())){
						///System.out.println(" CREATING PARENT " + cachedPath.getParent());
					   Files.createDirectories( cachedPath.getParent());
					}
					Files.copy( tempPath, cacheFile.toPath());
					////System.out.println(" MAPPED URL IS " + mappedURL);

					///return getConnection(fileHandler, mappedURL );
					return new CachingJarConnection( jarURL, mappedURL );
				}
			} catch(Exception exc) {
				System.out.println( " TROUBLE OPENING CONNECTION " + exc.getLocalizedMessage());
				exc.printStackTrace();
				throw new IOException("Trouble caching connection " + jarURL , exc);
			}
		}
		
		
		protected Path tempDir() throws IOException {
			if(_tempDir == null) {
			   Path currentDir = FileSystems.getDefault().getPath(System.getProperty("user.dir"));
			   _tempDir = Files.createTempDirectory(currentDir, "satis_cache");
			}
			return _tempDir;
		}

	}

}
