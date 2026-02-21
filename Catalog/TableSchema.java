package Catalog;

import java.util.List;
import AttributeInfo.Attribute;

public class TableSchema {
    private String tableName;
    private List<Attribute> attributes;
    private int rootPageID;

    // Constructor used by Parsers/Executors. PageID is set to default because yall don't have access to that info from there
    public TableSchema(String tableName, List<Attribute> attributes) {
        this.tableName = tableName;
        this.attributes = attributes;
        this.rootPageID = -1; // default value when table is created. Means that a page ID has not been allocated to this yet
    }

    // This 2nd constructor is for the Catalog itself. Specifically when the loadDisk function
    public TableSchema(String tableName, List<Attribute> attributes, int rootPageID) {
        this.tableName = tableName.toLowerCase();
        this.attributes = attributes;
        this.rootPageID = rootPageID;
    }
    public String getTableName() {
        return tableName;
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public int getRootPageID() {
        return rootPageID;
    }

    public void setRootPageID(int id) {
        this.rootPageID = id;
    }

    public int getRecordSize() {
        int size = 0;
        for (Attribute attr : attributes) {
            size += attr.getDefinition().getByteSize();
        }
        return size;
    }

    public void addAttribute(Attribute attr) {
        for (Attribute a : attributes) {
            if (a.getName().equalsIgnoreCase(attr.getName())) {
                throw new RuntimeException("Attribute already exists");
            }
        }
        attributes.add(attr);
    }

    public void dropAttribute(String attrName) {
        attributes.removeIf(a ->
                a.getName().equalsIgnoreCase(attrName)
        );
    }

    public void renameTable(String newTableName) {
        tableName = newTableName.toLowerCase();
    }

}