package qteam.solutions.s3;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.utils.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that connects to a specific S3 bucket
 * The class auto close the connection with S3
 * Use try-with-resources to instantiate it
 */
public class S3Interface implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(S3Interface.class);
    
    private final S3Client s3Client;
    private final String bucketName;
    private final Path downloadFolder;
    
    private final ExecutorService executor;

    /**
     * Creates a new S3Interface for the specified bucket and region
     * @param bucketName the bucket to operate on
     * @param region the region of the bucket
     * @throws S3InterfaceException if the bucket is empty or does not exist or if any S3 connection error occurs
     */
    public S3Interface(String bucketName, Region region) throws S3InterfaceException {
        try {
        	//instantiate the s3 client
            this.s3Client = S3Client.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
            
            //validate the bucket
            this.bucketName = bucketName;
            validateBucket();
            
            //set the download folder and create it
            this.downloadFolder = Paths.get(S3InterfaceHelper.getBaseDownloadFolder().toString(), 
            	bucketName);
            S3InterfaceHelper.createFolder(downloadFolder, Optional.empty());
            
            //set a executor used for folder downloads
            this.executor = Executors.newFixedThreadPool(11);

            logger.info("Successfully initialized S3Interface for bucket: {}", bucketName);
        } catch (S3Exception e) {
            logger.error("Error initializing S3Interface", e);
            throw new S3InterfaceException("Error initializing S3Interface", e);
        } catch (S3InterfaceException e) {
        	throw e;
        } catch (Exception e) {
        	logger.error("Error initializing S3Interface", e);
            throw new S3InterfaceException("Error initializing S3Interface", e);
        }
    }
    
    /**
     * Lists the files in the specified folder resource at the specified cursor
     * @param parent the folder resource to be listed
     * @param cursor used in case of paginated calls, the cursor keeps track of the listFolder calls
     * <br>use null for first time call
     * @return A ListResult containing the list of resources from the folder along with the cursor if exists
     * @throws S3InterfaceException if the provided resource is not a folder,
     * if the folder does not exist in the bucket,
     * or if any S3 connection error occurs 
     */
    public ListResult<Resource> listFolder(Resource parent, String cursor) throws S3InterfaceException {
    	//check the input to be a folder
    	if (parent != null && parent.getType() == 0) {
    		logger.error("The provided resource is not a folder");
    		throw new S3InterfaceException("The provided resource is not a folder");
    	}
    	
    	//set the folder to null in case of blank so all the files from the bucket will be listed
    	if (parent != null && 
    		(parent.getId().equals("/") || 
    		 StringUtils.isBlank(parent.getId()))) {
    		parent = null;
    	}
    	String folderName = (parent == null ? "/" : parent.getId());
    	
    	//create the request setting bucket, prefix and cursor if necessary
        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
        	.bucket(bucketName);
        if (parent != null) {
            requestBuilder.prefix(parent.getId());
        }
        if (cursor != null) {
            requestBuilder.continuationToken(cursor);
        }
        
        try {
        	//send the list request to S3 and throw exception if empty response
	        ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());

	        if (response.contents().size() == 0) {
	        	logger.error("The folder is empty or does not exist: {}", parent.getId());
    			throw new S3InterfaceException("The folder is empty or does not exist: " + folderName);
	        }

	        //transfer the content to a ListResult and return it
	        List<Resource> resources = response.contents().stream()
	        	.map(s3Object -> S3InterfaceHelper.createResourceFromKey(s3Object.key()))
	        	.collect(Collectors.toList());

	        logger.info("Successfully retrieved folder contents : {}", folderName);
	
	        return new ListResult<>(resources, response.nextContinuationToken());
        } catch (SdkException e) {
            logger.error("Error listing folder {} for bucket: {}", folderName, bucketName, e);
            throw new S3InterfaceException("Error listing folder " + folderName + " for bucket: " + bucketName, e);
        }
    }

    /**
     * Gets a resource (file or folder) from the bucket
     * @param id the key of the resource
     * @return a Resource instance 
     * @throws S3InterfaceException if the input key is blank,
     * if the resource is not present in the bucket,
     * or if any S3 connection error occurs
     */
    public Resource getResource(String id) throws S3InterfaceException {
    	//check the input is not empty
    	if (StringUtils.isBlank(id)) {
    		logger.error("The id of the resource cannot be empty");
    		throw new S3InterfaceException("The id of the resource cannot be empty");
    	}
        
        try {
        	if (S3InterfaceHelper.isFolder(id)) {
        		//in case we are checking a folder create a list request and check is not empty
        		ListObjectsV2Request request = ListObjectsV2Request.builder()
        	        .bucket(bucketName)
        	        .prefix(id)
        	        .maxKeys(1)
        	        .build();
        		if (s3Client.listObjectsV2(request).contents().size() == 0) {
        			logger.error("Resource with ID: {} not found", id);
        			throw new S3InterfaceException("Resource with ID: " + id + " not found");
        		}
        	} else {
        		//in case we are a checking a file create a head request and check no exception is thrown
        		HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(id)
                    .build();
        		s3Client.headObject(request);
        	}
        	logger.info("Successfully retrieved resource with ID : {}", id);
        	
        	//convert the key to a resource and return it
            return S3InterfaceHelper.createResourceFromKey(id);
        } catch (NoSuchKeyException e) {
        	logger.error("Resource with ID: {} not found", id);
        	throw new S3InterfaceException("Resource with ID: " + id + " not found", e);
        } catch (S3Exception e) {
        	logger.error("Failed to check resource with ID: {}", id);
            throw new S3InterfaceException("Failed to check resource with ID: " + id, e);
        }
    }

    /**
     * Downloads a specified resource (file or folder) from the S3 bucket in the download folder
     * @param resource the resource to be downloaded, if null it will download all the contents in the bucket
     * @return a File instance pointing to the downloaded resource
     * @throws S3InterfaceException if the resource does not exist, 
     * if any IO exception occurs 
     * or if any S3 connection error occurs
     */
    public File getAsFile(Resource resource) throws S3InterfaceException {
    	//if the resource id is blank set the resource to null so all the bucket will be downloaded
    	if (resource != null && 
    		(resource.getId().equals("/") || 
    		 StringUtils.isBlank(resource.getId()))) {
    		resource = null;
    	}
    	//if the resource is null or folder call downloadFolder else call downloadFile
    	if (resource == null || resource.getType() == 1) {
    		return downloadFolder(resource);
    	} else {
    		return downloadFile(resource);
    	}
    }
    
    @Override
	public void close() throws Exception {
    	//close the S3 client
		if (s3Client != null) {
			s3Client.close();
		}
		//close the executor
		if (executor != null && !executor.isShutdown()) {
	        executor.shutdown();
	        try {
	            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
	                executor.shutdownNow();
	            }
	        } catch (InterruptedException e) {
	            executor.shutdownNow();
	            Thread.currentThread().interrupt();
	        }
	    }
	}
    
    private void validateBucket() throws S3InterfaceException {
        try {
        	//to validate a bucket first send a head request to check that no exception is thrown
            HeadBucketRequest request = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();
            s3Client.headBucket(request);
            
            //second send a dummy list request to check the bucket is not empty
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .maxKeys(1)
                .build();
            if (s3Client.listObjectsV2(listRequest).contents().size() == 0) {
            	logger.error("The bucket is empty: {}", bucketName);
            	throw new S3InterfaceException("The bucket is empty: " + bucketName);
            }
            
            logger.info("Successfully validated access to bucket: {}", bucketName);
        } catch (NoSuchBucketException e) {
            logger.error("Bucket does not exist: {}", bucketName);
            throw new S3InterfaceException("Bucket does not exist: " + bucketName, e);
        } catch (SdkException e) {
            logger.error("Error accessing bucket: {}", bucketName, e);
            throw new S3InterfaceException("Error accessing bucket: " + bucketName, e);
        }
    }
    
    private File downloadFile(Resource resource) throws S3InterfaceException {
    	//create the output path
    	Path filePath = Paths.get(downloadFolder.toString(), resource.getId());
    	
    	//create a get object request
    	GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(resource.getId())
                .build();

        try (var inputStream = s3Client.getObject(request)) {
        	//copy contents from S3 to local file
            Files.copy(inputStream, filePath, StandardCopyOption.REPLACE_EXISTING);
            logger.info("Successfully downloaded file: {}", filePath);
            
            //return the file
            return filePath.toFile();
        } catch (IOException e) {
        	logger.error("Failed to download file: {}", resource.getId());
            throw new S3InterfaceException("Failed to download file: " + resource.getId(), e);
        }
    }
    
    private File downloadFolder(Resource resource) throws S3InterfaceException {
    	//create the output folder path
    	Path folderPath = (resource == null) ?
    					  downloadFolder :
    					  Paths.get(downloadFolder.toString(), resource.getId());
    	String folderName = (resource == null ? "/" : resource.getId());
    	logger.info("Started downloading folder: {}", folderName);
    	
    	//init counters 
    	int countTotal = 0;
    	AtomicInteger countDownloaded = new AtomicInteger();
    	AtomicInteger countFolders = new AtomicInteger();
    	
    	String cursor = null;
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        do {
        	//get the list of files from the S3 folder
            ListResult<Resource> fileList = listFolder(resource, cursor);
            //create the output folder
            S3InterfaceHelper.createFolder(folderPath, Optional.of(countFolders));
            //increment file counter
            countTotal += fileList.getResources().size();
            
            //for every file resource asynchronously download each one
            for (Resource file : fileList.getResources()) {
            	futures.add(CompletableFuture.runAsync(
            		() -> handleAsyncDownload(file, countDownloaded, countFolders), executor));
            }
            //in case there are more files on the folder
            cursor = fileList.getCursor();
        } while (cursor != null);
        
        //wait for all the async calls to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        
        logger.info("Successfully downloaded folder: {}", folderName);
        logger.info("{} of {} files downloaded, {} folders created", countDownloaded.get(), countTotal, countFolders.get());
        
        //return the folder
        return folderPath.toFile();
    }
    
    private void handleAsyncDownload(Resource resource, AtomicInteger countDownloaded, AtomicInteger countFolders) {
    	try {
    		//extract the folder name from a file resource key and create it if not exists
    		String folderName = S3InterfaceHelper.extractFolderName(resource);
    		if (folderName != null) {
    			S3InterfaceHelper.createFolder(Paths.get(downloadFolder.toString(), folderName), Optional.of(countFolders));
    		}
    		
    		//download the file and increment the counter
	        downloadFile(resource);
	        countDownloaded.incrementAndGet();
	    } catch (S3InterfaceException e) {}
    }
}