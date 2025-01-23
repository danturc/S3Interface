package qteam.solutions.s3;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3InterfaceHelper {
	private static final Logger logger = LoggerFactory.getLogger(S3InterfaceHelper.class);

	public static void createFolder(Path folder, Optional<AtomicInteger> count) throws S3InterfaceException {
    	try {
    		if (!Files.exists(folder)) {
    			Files.createDirectories(folder);
    			if (count.isPresent()) {
    				count.get().incrementAndGet();
    			}
    			logger.info("Successfully created folder: {}", folder);
    		}
    	} catch (Exception e) {
            logger.error("Error creating folder: {}", folder);
            throw new S3InterfaceException("Error creating folder: " + folder);
        }
    }
	
	public static String extractFolderName(Resource file) {
		if (file == null) return null;
		int idx = file.getId().lastIndexOf("/");
		return idx == -1 ? null : file.getId().substring(0, idx);
	}
	
	public static boolean isFolder(String key) {
		return key.endsWith("/");
	}
    
    public static Resource createResourceFromKey(String key) {
    	boolean isFolder = isFolder(key);
    	
    	String auxKey = isFolder ? key.substring(0, key.length() - 1) : key;
    	String name = auxKey.substring(auxKey.lastIndexOf('/') + 1);
    	
    	return new Resource(key, name, isFolder ? 1 : 0);
    }
    
    public static Path getBaseDownloadFolder() {
        Properties properties = new Properties();
        Path downLoadFolder = null;
        
        try (FileInputStream input = new FileInputStream("src/main/resources/application.properties")) {
            properties.load(input);
            String property = properties.getProperty("download.folder");
            
            String regex = "\\$\\{([^}]+)\\}";
            Pattern pattern = Pattern.compile(regex);
            
            Matcher matcher = pattern.matcher(property);
            while (matcher.find()) {
                String propertyName = matcher.group(1);  // Extract the property name
                String propertyValue = System.getProperty(propertyName);  // Get the property value
                
                if (propertyValue != null) {
                    property = property.replace(matcher.group(0), propertyValue);
                    matcher = pattern.matcher(property);  // Re-run the matcher to handle subsequent replacements
                }
            }

            downLoadFolder = Paths.get(property);
            
        } catch (IOException e) {
            logger.warn("Download folder cannot be found in application properties, using default", e);
            downLoadFolder = Paths.get(System.getProperty("user.home"), "qteam/download");
        }
        return downLoadFolder;
    }
}