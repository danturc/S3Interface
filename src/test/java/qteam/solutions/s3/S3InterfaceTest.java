package qteam.solutions.s3;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;

public class S3InterfaceTest {

    private final S3Client mockS3Client = mock(S3Client.class);
    private final S3ClientBuilder mockBuilder = mock(S3ClientBuilder.class);
    private final DefaultCredentialsProvider mockCredentialsProvider = mock(DefaultCredentialsProvider.class);
    private final ListObjectsV2Response mockListObjectsV2Response = mock(ListObjectsV2Response.class);

    private S3Interface getMockS3Interface() throws Exception {
    	try (MockedStatic<S3Client> mockS3ClientStatic = 
    			 mockStatic(S3Client.class);
    		 MockedStatic<DefaultCredentialsProvider> mockCredentialsProviderStatic = 
    			 mockStatic(DefaultCredentialsProvider.class);
    		 MockedStatic<S3InterfaceHelper> mockS3InterfaceHelperStatic = 
    			 mockStatic(S3InterfaceHelper.class)) {

    		mockS3ClientStatic.when(S3Client::builder)
    			.thenReturn(mockBuilder);
    		mockCredentialsProviderStatic.when(DefaultCredentialsProvider::create)
    			.thenReturn(mockCredentialsProvider);
    		mockS3InterfaceHelperStatic.when(S3InterfaceHelper::getBaseDownloadFolder)
				.thenReturn(Path.of("mock/path"));
    		mockS3InterfaceHelperStatic.when(() -> S3InterfaceHelper.createFolder(any(Path.class), any(Optional.class)))
    			.thenAnswer(invocation -> null);

    		when(mockBuilder.region(any(Region.class)))
    			.thenReturn(mockBuilder);
    		when(mockBuilder.credentialsProvider(any(AwsCredentialsProvider.class)))
    			.thenReturn(mockBuilder);
    		when(mockBuilder.build())
    			.thenReturn(mockS3Client);

    		when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
    			.thenAnswer(invocation -> null);
    		when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
    			.thenReturn(mockListObjectsV2Response);
    		when(mockListObjectsV2Response.contents())
    			.thenReturn(List.of(S3Object.builder().key("dummy").build()));

    		return new S3Interface("test-bucket", Region.US_EAST_1);
    	}
    }

    @Test
    public void testConstructorSuccess() throws Exception {
    	try (MockedStatic<S3Client> mockS3ClientStatic = 
    			 mockStatic(S3Client.class);
             MockedStatic<DefaultCredentialsProvider> mockCredentialsProviderStatic = 
            	 mockStatic(DefaultCredentialsProvider.class);
             MockedStatic<S3InterfaceHelper> mockS3InterfaceHelperStatic = 
            	 mockStatic(S3InterfaceHelper.class)) {

			mockS3ClientStatic.when(S3Client::builder)
				.thenReturn(mockBuilder);
			mockCredentialsProviderStatic.when(DefaultCredentialsProvider::create)
				.thenReturn(mockCredentialsProvider);
			mockS3InterfaceHelperStatic.when(() -> S3InterfaceHelper.createFolder(any(Path.class), any(Optional.class)))
				.thenAnswer(invocation -> null);
			
			when(mockBuilder.region(any(Region.class)))
				.thenReturn(mockBuilder);
			when(mockBuilder.credentialsProvider(any(AwsCredentialsProvider.class)))
				.thenReturn(mockBuilder);
			when(mockBuilder.build())
				.thenReturn(mockS3Client);
			   
			mockS3InterfaceHelperStatic.when(S3InterfaceHelper::getBaseDownloadFolder)
				.thenReturn(Path.of("mock/path"));
			   	
			when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
				.thenAnswer(invocation -> null);
			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
				.thenReturn(mockListObjectsV2Response);
			when(mockListObjectsV2Response.contents())			
				.thenReturn(List.of(S3Object.builder().key("dummy").build()));
			
			try (S3Interface s3Interface = new S3Interface("test-bucket", Region.US_EAST_1)) {}
			
			ArgumentCaptor<Path> folderCaptor = ArgumentCaptor.forClass(Path.class);
			mockS3InterfaceHelperStatic.verify(
				() -> S3InterfaceHelper.createFolder(folderCaptor.capture(), any(Optional.class)), times(1));
			
			assertEquals(Paths.get("mock", "path", "test-bucket"), folderCaptor.getValue());
        }
    }

    @Test
    public void testConstructorValidateBucketFails() throws Exception {
    	try (MockedStatic<S3Client> mockS3ClientStatic = 
    			 mockStatic(S3Client.class);
             MockedStatic<DefaultCredentialsProvider> mockCredentialsProviderStatic = 
            	 mockStatic(DefaultCredentialsProvider.class);
             MockedStatic<S3InterfaceHelper> mockS3InterfaceHelperStatic = 
            	 mockStatic(S3InterfaceHelper.class)) {

   			mockS3ClientStatic.when(S3Client::builder)
   				.thenReturn(mockBuilder);
   			mockCredentialsProviderStatic.when(DefaultCredentialsProvider::create)
   				.thenReturn(mockCredentialsProvider);
   			mockS3InterfaceHelperStatic.when(() -> S3InterfaceHelper.createFolder(any(Path.class), any(Optional.class)))
   				.thenAnswer(invocation -> null);
   			
   			when(mockBuilder.region(any(Region.class)))
   				.thenReturn(mockBuilder);
   			when(mockBuilder.credentialsProvider(any(AwsCredentialsProvider.class)))
   				.thenReturn(mockBuilder);
   			when(mockBuilder.build())
   				.thenReturn(mockS3Client);
   			   
   			mockS3InterfaceHelperStatic.when(S3InterfaceHelper::getBaseDownloadFolder)
   				.thenReturn(Path.of("mock/path"));
   			   	
   			when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
   				.thenAnswer(invocation -> null);
   			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
   				.thenThrow(new RuntimeException("Bucket validation failed"));
   			
   			assertThrows(S3InterfaceException.class, 
   				() -> new S3Interface("test-bucket", Region.US_EAST_1));
        }
    }

    @Test
    public void testConstructorCreateFolderFails() throws Exception {
    	try (MockedStatic<S3Client> mockS3ClientStatic = 
    			 mockStatic(S3Client.class);
    		 MockedStatic<DefaultCredentialsProvider> mockCredentialsProviderStatic = 
    			 mockStatic(DefaultCredentialsProvider.class);
    		 MockedStatic<S3InterfaceHelper> mockS3InterfaceHelperStatic = 
    			 mockStatic(S3InterfaceHelper.class)) {

    		mockS3ClientStatic.when(S3Client::builder)
    			.thenReturn(mockBuilder);
    		mockCredentialsProviderStatic.when(DefaultCredentialsProvider::create)
    			.thenReturn(mockCredentialsProvider);
    		mockS3InterfaceHelperStatic.when(() -> S3InterfaceHelper.createFolder(any(Path.class), any(Optional.class)))
    			.thenThrow(new RuntimeException("Folder creation failed"));

    		when(mockBuilder.region(any(Region.class)))
    			.thenReturn(mockBuilder);
    		when(mockBuilder.credentialsProvider(any(AwsCredentialsProvider.class)))
    			.thenReturn(mockBuilder);
    		when(mockBuilder.build())
    			.thenReturn(mockS3Client);

    		mockS3InterfaceHelperStatic.when(S3InterfaceHelper::getBaseDownloadFolder)
    			.thenReturn(Path.of("mock/path"));

    		when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
    			.thenAnswer(invocation -> null);
    		when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
				.thenReturn(mockListObjectsV2Response);
			when(mockListObjectsV2Response.contents())
				.thenReturn(List.of(S3Object.builder().key("dummy").build()));

    		assertThrows(S3InterfaceException.class, 
    			() -> new S3Interface("test-bucket", Region.US_EAST_1));
    	}
    }

    @Test
    public void testS3ExceptionInConstructor() throws Exception {
    	try (MockedStatic<S3Client> mockS3ClientStatic = 
    			 mockStatic(S3Client.class);
    		 MockedStatic<DefaultCredentialsProvider> mockCredentialsProviderStatic = 
    			 mockStatic(DefaultCredentialsProvider.class);
    		 MockedStatic<S3InterfaceHelper> mockS3InterfaceHelperStatic = 
    			 mockStatic(S3InterfaceHelper.class)) {

    		mockS3ClientStatic.when(S3Client::builder)
    			.thenReturn(mockBuilder);
    		mockCredentialsProviderStatic.when(DefaultCredentialsProvider::create)
    			.thenReturn(mockCredentialsProvider);

    		when(mockBuilder.region(any(Region.class)))
    			.thenReturn(mockBuilder);
    		when(mockBuilder.credentialsProvider(any(AwsCredentialsProvider.class)))
    			.thenReturn(mockBuilder);
    		when(mockBuilder.build())
    			.thenThrow(S3Exception.builder().message("Simulated S3 error").build());

    		assertThrows(S3InterfaceException.class, 
    			() -> new S3Interface("test-bucket", Region.US_EAST_1));
    	}
    }

    @Test
    public void testListFolderSuccess() throws Exception {
        try (S3Interface s3Interface = getMockS3Interface()) {
            S3Object s3Object1 = S3Object.builder()
            	.key("folder1/file1.txt").size(100L).build();
            S3Object s3Object2 = S3Object.builder()
            	.key("folder1/file2.txt").size(200L).build();
            S3Object s3Object3 = S3Object.builder()
                	.key("folder1/file3.txt").size(300L).build();
            ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
                .contents(s3Object1, s3Object2, s3Object3)
                .nextContinuationToken("next-token")
                .build();

            when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
            	.thenReturn(mockResponse);

            ListResult<Resource> result = s3Interface
            	.listFolder(new Resource("folder1", "folder1", 1), "cursor");

            assertNotNull(result);
            assertEquals(3, result.getResources().size());
            assertEquals("next-token", result.getCursor());
        }

        verify(mockS3Client, times(2)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    public void testListFolderWithInvalidResourceType() throws Exception {
    	try (S3Interface s3Interface = getMockS3Interface()) {
			Resource invalidResource = new Resource("invalidResource", "invalidResource", 0);

			assertThrows(S3InterfaceException.class, 
				() -> s3Interface.listFolder(invalidResource, null));
		}
    }

    @Test
    public void testListFolderWithS3Exception() throws Exception {
    	try (S3Interface s3Interface = getMockS3Interface()) {
			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
			    .thenThrow(S3Exception.builder().message("Simulated S3 error").build());

			assertThrows(S3InterfaceException.class, 
				() -> s3Interface.listFolder(new Resource("folder1", "folder", 1), null));
		}
    }

    @Test
    public void testGetResourceFile() throws Exception {
    	try (S3Interface s3Interface = getMockS3Interface()) {
	        HeadObjectResponse mockResponse = HeadObjectResponse.builder()
	            .contentLength(100L)
	            .build();
	        
	        when(mockS3Client.headObject(any(HeadObjectRequest.class)))
	        	.thenReturn(mockResponse);
	
	        Resource result = s3Interface.getResource("file1.txt");
	
	        assertNotNull(result);
	        assertEquals("file1.txt", result.getId());
	        assertEquals("file1.txt", result.getName());
	        assertEquals(0, result.getType());
    	}
    }

    @Test
    public void testGetResourceFolder() throws Exception {
    	try (S3Interface s3Interface = getMockS3Interface()) {
	        HeadObjectResponse mockResponse = HeadObjectResponse.builder()
	            .contentLength(0L)
	            .build();
	        
	        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenReturn(mockResponse);
	
	        Resource result = s3Interface.getResource("folder1/");
	
	        assertNotNull(result);
	        assertEquals("folder1/", result.getId());
	        assertEquals("folder1", result.getName());
	        assertEquals(1, result.getType());
    	}
    }

    @Test
    public void testGetResourceEmptyId() throws Exception {
    	try (S3Interface s3Interface = getMockS3Interface()) {
	        assertThrows(S3InterfaceException.class, 
	        	() -> s3Interface.getResource(""));
	        assertThrows(S3InterfaceException.class, 
	        	() -> s3Interface.getResource("  "));
	        assertThrows(S3InterfaceException.class, 
	        	() -> s3Interface.getResource(null));
    	}
    }

    @Test
    public void testGetResourceNoSuchKeyException() throws Exception {
    	try (S3Interface s3Interface = getMockS3Interface()) {
	        when(mockS3Client.headObject(any(HeadObjectRequest.class)))
	            .thenThrow(NoSuchKeyException.class);

	        assertThrows(S3InterfaceException.class, 
	        	() -> s3Interface.getResource("nonexistent-file"));
    	}
    }

    @Test
    public void testGetResourceS3Exception() throws Exception {
    	try (S3Interface s3Interface = getMockS3Interface()) {
	        when(mockS3Client.headObject(any(HeadObjectRequest.class)))
	            .thenThrow(S3Exception.builder().message("S3 simulated error").build());

	        assertThrows(S3InterfaceException.class, 
	        	() -> s3Interface.getResource("file1.txt"));
    	}
    }

    @Test
    public void testGetAsFileSuccess_File() throws Exception {
        try (S3Interface s3Interface = getMockS3Interface()) {
        	Resource resource = new Resource("file1", "file1", 0);
        	Path mockFilePath = Paths.get("mock/path/test-bucket/file1");

        	var mockInputStream = mock(ResponseInputStream.class);

            try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
                mockedFiles
                	.when(() -> Files.copy(any(InputStream.class), any(Path.class), any(CopyOption.class)))
                    .thenAnswer(invocation -> null);

                when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(mockInputStream);

                File downloadedFile = s3Interface.getAsFile(resource);

                assertNotNull(downloadedFile);
                assertEquals(mockFilePath.toFile(), downloadedFile);

                mockedFiles.verify(
                	() -> Files.copy(any(InputStream.class), any(Path.class), any(CopyOption.class)), times(1));
                verify(mockS3Client, times(1)).getObject(any(GetObjectRequest.class));
            }
        }
    }

    @Test
    public void testDownloadFileFailure() throws Exception, S3InterfaceException {
        try (S3Interface s3Interface = getMockS3Interface()) {
        	Resource resource = new Resource("file1", "file1", 0);

        	var mockInputStream = mock(ResponseInputStream.class);

            try (MockedStatic<Files> mockedFiles = mockStatic(Files.class)) {
                mockedFiles
                	.when(() -> Files.copy(any(InputStream.class), any(Path.class), any(CopyOption.class)))
                    .thenThrow(new IOException("Unknown IO error"));

                when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(mockInputStream);

                assertThrows(S3InterfaceException.class, 
        	        	() -> s3Interface.getAsFile(resource));
            }
        }
    }

    @Test
    public void testGetAsFileSuccess_Folder() throws Exception, S3InterfaceException {
        try (S3Interface s3Interface = getMockS3Interface()) {
        	Resource resource = new Resource("folder1/", "folder1", 1);
        	Path mockFilePath = Paths.get("mock/path/test-bucket/folder1");
        	
        	S3Object s3Object1 = S3Object.builder()
            	.key("folder1/folder2/file1").size(100L).build();
            S3Object s3Object2 = S3Object.builder()
            	.key("folder1/folder2/file2").size(200L).build();
            S3Object s3Object3 = S3Object.builder()
            	.key("folder1/file3").size(100L).build();
            S3Object s3Object4 = S3Object.builder()
            	.key("folder1/file4").size(200L).build();
            ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
                    .contents(s3Object1, s3Object2, s3Object3, s3Object4)
                    .build();

        	var mockInputStream = mock(ResponseInputStream.class);

            try (MockedStatic<Files> mockedFiles = mockStatic(Files.class);
            	 MockedStatic<S3InterfaceHelper> mockedS3Helper = mockStatic(S3InterfaceHelper.class)) {
                mockedFiles
                	.when(() -> Files.copy(any(InputStream.class), any(Path.class), any(CopyOption.class)))
                    .thenAnswer(invocation -> null);
                mockedS3Helper.when(() -> S3InterfaceHelper.createFolder(any(Path.class), any(Optional.class)))
                	.thenAnswer(invocation -> null);
                mockedS3Helper.when(() -> S3InterfaceHelper.createResourceFromKey(any(String.class)))
            		.thenCallRealMethod();

                when(mockS3Client.getObject(any(GetObjectRequest.class)))
                	.thenReturn(mockInputStream);
                when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
	            	.thenReturn(mockResponse);

                File downloadedFolder = s3Interface.getAsFile(resource);

                assertNotNull(downloadedFolder);
                assertEquals(mockFilePath.toFile(), downloadedFolder);
            }
        }
    }
}