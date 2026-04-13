package Common;
import AttributeInfo.AttributeTypeEnum;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class BTreeNode implements Pages{
    private AttributeTypeEnum searchKeyType;
    private int numEntries;
    private int address;
    private boolean internal;
    //address which you can use to search through bufferPages
    private Integer myParent;
    //-1 if no address
    private Map<Object, Integer> IndexEntries;
    private Instant lastUsed;
    private boolean modified;
    public BTreeNode(int numEntries, int address, boolean modified, boolean internal, Integer myParent, AttributeTypeEnum searchKeyType){
        this.numEntries = numEntries;
        this.address = address;
        this.modified = modified;
        this.IndexEntries = new HashMap<>();
        this.lastUsed = Instant.now();
        this.internal = internal;
        this.myParent = myParent;
        this.searchKeyType = searchKeyType;
    }

    public int getPageAddress(){return this.address;}
    public void SetModified(boolean modified){this.modified = modified;}
    public boolean getModified(){return this.modified;}
    public Instant getLastUsed(){return this.lastUsed;}
    public void updateLastUsed(){this.lastUsed = Instant.now();}
    public Map<Object, Integer> getIndexEntries(){return this.IndexEntries;}
    public AttributeTypeEnum getSearchKeyType(){return this.searchKeyType;}
    public int getNumEntries(){return this.numEntries;}
    public Integer getMyParent(){return this.myParent;}
    //the delete a node entirely set myparent to null
    public void setMyParent(int myParent){this.myParent = myParent;}
    public boolean isInternal(){return this.internal;}
    public void insertIndex(Object searchKey, int address){
        this.IndexEntries.put(searchKey, address);
        this.numEntries++;
    }
    public void deleteIndex(Object searchKey){
        this.IndexEntries.remove(searchKey);
        this.numEntries--;
    }
}
