package qteam.solutions.s3;

import java.util.List;

class ListResult<T> {
    private List<T> resources;
    private String cursor;

    public ListResult(List<T> resources, String cursor) {
        this.resources = resources;
        this.cursor = cursor;
    }

    public List<T> getResources() {
        return resources;
    }

    public String getCursor() {
        return cursor;
    }
}