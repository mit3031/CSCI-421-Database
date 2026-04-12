package Catalog;

import AttributeInfo.AttributeTypeEnum;

public class BTreeSchema {
    private int rootNodeAddress;
    private int treeOrderN; // The n value - max pointers per node
    private boolean isPrimaryKey;
    private boolean hasNull;
    private AttributeTypeEnum indexedType;

    public BTreeSchema(int treeOrderN, boolean isPrimaryKey, boolean hasNull, AttributeTypeEnum indexedType) {
        this.rootNodeAddress = -1;
        this.treeOrderN = treeOrderN;
        this.isPrimaryKey = isPrimaryKey;
        this.hasNull = hasNull;
        this.indexedType = indexedType;
    }

    // Getters and Setters
    public int getRootNodeAddress() {
        return rootNodeAddress;
    }

    public void setRootNodeAddress(int rootNodeAddress) {
        this.rootNodeAddress = rootNodeAddress;
    }

    public int getTreeOrderN() {
        return treeOrderN;
    }

    public boolean getIsPrimaryKey() {
        return isPrimaryKey;
    }

    public boolean getHasNull() {
        return hasNull;
    }

    public AttributeTypeEnum getIndexedType() {
        return indexedType;
    }
}
