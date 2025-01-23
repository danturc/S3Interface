package qteam.solutions.s3;

public class Resource {
    private String id;
    private String name;
    private int type;

    public Resource(String id, String name, int type) {
        this.id = id;
        this.name = name;
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }
    
    @Override
    public String toString() {
    	return id + " - " + (type == 1 ? "folder" : "file");
    }
}