package qteam.solutions.s3;

import software.amazon.awssdk.regions.Region;

public class S3Test {
    public static void main(String[] args) {
    	String bucketName = "qteam-task-bucket";
        Region region = Region.EU_NORTH_1;
        
        try (S3Interface s3Interface = new S3Interface(bucketName, region)) {
        	System.out.println();
        	System.out.println("*************************************************");
        	System.out.println();
        	
        	s3Interface.getAsFile(new Resource("poze/", "poze", 1));
        	
        	
        	System.out.println();
        	System.out.println("*************************************************");
        	System.out.println();
        	
        	s3Interface.getAsFile(null);
        	
        	System.out.println();
        	System.out.println("*************************************************");
        	System.out.println();
        	
        	System.out.println("Resources in bucket:");
        	String cursor = null;
        	do {
                ListResult<Resource> fileList = s3Interface.listFolder(null, cursor);
                for (Resource file : fileList.getResources()) {
                	System.out.println(file.getId());
                }
                cursor = fileList.getCursor();
            } while (cursor != null);
            
            System.out.println();
            System.out.println("*************************************************");
        	System.out.println();
        	
        	System.out.println("Resources in folder:");
        	cursor = null;
        	do {
                ListResult<Resource> fileList = s3Interface.listFolder(new Resource("poze/", "poze", 1), cursor);
                for (Resource file : fileList.getResources()) {
                	System.out.println(file.getId());
                }
                cursor = fileList.getCursor();
            } while (cursor != null);
            
            System.out.println();
            System.out.println("*************************************************");
            System.out.println();
            
            Resource resource = s3Interface.getResource("poze/lp/Jungle Fever.jpeg");
            System.out.println("Resource file details: " + resource);
            
            System.out.println();
            System.out.println("*************************************************");
            System.out.println();
            
            resource = s3Interface.getResource("poze/");
            System.out.println("Resource folder details: " + resource);
            
            System.out.println();
            System.out.println("*************************************************");
            System.out.println();

            resource = s3Interface.getResource("inexistent");
            System.out.println("Resource details: " + resource);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}