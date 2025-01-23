Task: AWS S3 Interface Implementation
You are required to implement a Java class (or multiple classes) that interfaces with AWS S3. The task should be addressed in an efficient, clean, and effective manner, adhering to best practices for authentication, exception handling, and resource management with AWS S3.

Requirements:
You are to write a class that interfaces with a single AWS S3 bucket provided in the constructor. The following methods need to be implemented:

public ListResult<Resource> listFolder(Resource parent, String cursor)

Lists the contents of a given parent resource.
Supports pagination.
Parameters:
parent: The parent resource (could be null).
cursor: The pagination cursor.
public Resource getResource(String id)

Returns the resource corresponding to the specified ID.
Parameters:
id: The ID of the resource (cannot be null).
public File getAsFile(Resource resource)

Downloads the specified resource and returns it as a File object.
Parameters:
resource: The resource to be downloaded (cannot be null).
Supporting Classes:

class Resource {
    private String id;
    private String name;
    private int type;  // 0 for file, 1 for folder
}

class ListResult<T> {
    private List<T> resources;
    private String cursor;  // Used for pagination
}

Notes:
Ensure that your implementation follows best practices for interfacing with AWS S3.
Handle authentication, exception handling, and resource management efficiently.
We value candidates who demonstrate curiosity, integrity, and skillfulness in their approach.
