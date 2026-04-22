package Common;
import AttributeInfo.AttributeTypeEnum;
import Catalog.Catalog;
import StorageManager.StorageManager;
import StorageManager.BufferManager;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

public class BTreeNode implements Pages{
    private AttributeTypeEnum searchKeyType;
    //THIS IS N: Max ENTRIES
    private int numEntries;
    private int address;
    private boolean internal;
    //address which you can use to search through bufferPages
    private Integer myParent;
    private Integer lastPoint;
    //-1 if no address
    private Map<Object, Integer> IndexEntries;
    private Instant lastUsed;
    private boolean modified;
    public BTreeNode(int numEntries, int address, boolean modified, boolean internal, Integer myParent, AttributeTypeEnum searchKeyType, Integer lastPoint){
        this.numEntries = numEntries;
        this.address = address;
        this.modified = modified;
        this.IndexEntries = new TreeMap<>(new KeyCompare());
        this.lastUsed = Instant.now();
        this.internal = internal;
        this.myParent = myParent;
        this.searchKeyType = searchKeyType;
        this.lastPoint = lastPoint;
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
    public Integer getLastPoint(){return this.lastPoint;}
    public void setLastPoint(Integer lastPoint){this.lastPoint = lastPoint; modified = true;}
    //the delete a node entirely set myparent to null
    public void setMyParent(int myParent){this.myParent = myParent; modified = true;}
    public boolean isInternal(){return this.internal;}
    public void insertIndex(Object searchKey, int address){
        this.IndexEntries.put(searchKey, address);
        modified = true;
        update();
    }
    public void deleteIndex(Object searchKey){
        this.IndexEntries.remove(searchKey);
        modified = true;
        update();
    }

    /**
     * Finds the page that a given key should be inserted into, recrusivley calls itself until it returns a page address
     * @param searchKey the key we are trying to insert
     * @return the page to insert into
     */
    public int findPageToInsert(Object searchKey){
        BufferManager bufferManager = BufferManager.getInstance();

        try{
            for(Object nodeSearchKey : this.IndexEntries.keySet()){
                int searchKeyCompare = ((Comparable)searchKey).compareTo(nodeSearchKey);
                if(searchKeyCompare < 0 && !this.internal){
                    return this.IndexEntries.get(nodeSearchKey);
                } else if (searchKeyCompare < 0){
                    return bufferManager.readBTreeNode(this.IndexEntries.get(nodeSearchKey)).findPageToInsert(searchKey);
                }

                }
            if(!this.internal){
                return this.lastPoint;
            } else{
                return bufferManager.readBTreeNode(this.lastPoint).findPageToInsert(searchKey);
            }
        } catch(IOException e){
            Logger.log("Error while attempting to readBTreeNode");
            lastUsed = Instant.now();
            throw new RuntimeException(e);
        }

    }

    /**
     * Attempts to update b+ tree based on status of this node.
     * @Author: Antonio Bicknell
     */
    private void update() {
        int count = IndexEntries.size();
        //table is too large, split this one and propogate insert to parent
        if (count >= numEntries || ( !internal && count >= numEntries -1 ) ){
            //put time as latest possible so that it's not gonna leave memory
            try {
                lastUsed = Instant.MAX;
                //need new page slot, get from catalog
                Catalog cat = Catalog.getInstance();
                int newPage = cat.getFirstFreePage();
                //also remove it so we don't ruin everything
                cat.removeFirstFreePage();
                //create our node
                BufferManager bm = BufferManager.getInstance();

                bm.newBTreeNode(
                        newPage,
                        this.numEntries,
                        this.internal,
                        this.myParent,
                        this.searchKeyType,
                        this.lastPoint
                );
                this.lastPoint = newPage;
                BTreeNode newNode = bm.readBTreeNode(newPage);
                //split records, right half into new node, and remove
                ArrayList keys = new ArrayList<>(IndexEntries.keySet());
                Object newNodeMin = keys.get( (int)Math.ceil(keys.size()/2.0));
                for(int i = (int)Math.ceil(keys.size()/2.0); i<keys.size(); i++){
                    //copy 2nd half to new
                    int oldVal = IndexEntries.get(keys.get(i));
                    newNode.IndexEntries.put(keys.get(i), oldVal);
                    this.IndexEntries.remove(keys.get(i));
                }

                //update parents of new children nodes of new node
                for (int childIndex : newNode.IndexEntries.values()){
                    BTreeNode child = bm.readBTreeNode(childIndex);
                    child.setMyParent(newPage);
                }

                StorageManager sm = StorageManager.getStorageManager();

                //update up tree
                int parentToUpdate = this.myParent;

                if(parentToUpdate != -1){
                    Logger.log("Node was not parent, no additional creation necessary");
                    //case where we have a parent
                    BTreeNode parent = bm.readBTreeNode(parentToUpdate);

                    //What is the key here, last value of this page?
                    //todo figure out above comment
                    //Find node in parent that points to this currently, now should point to new node
                    boolean found = false;
                    for(Object k : parent.IndexEntries.keySet()){
                        if(parent.IndexEntries.get(k) == this.address) {
                            found = true;
                            parent.IndexEntries.put(k, newPage);
                        }
                    }
                    //if not found in that loop,was last entry
                    if(!found){
                        if(parent.lastPoint != address){
                            Logger.log("Potentially fatal Error: did not find address in parent when updating!");
                        }
                        parent.lastPoint = newPage;
                    }
                    //now put in the number that points to this
                    parent.IndexEntries.put(newNodeMin, this.address);



                }
                else{
                    Logger.log("Node was parent, must create new Root Node...");
                    //case where this was parent, no longer will be
                    int newHeadPage = cat.getFirstFreePage();
                    cat.removeFirstFreePage();
                    bm.newBTreeNode(
                            newHeadPage,
                            this.numEntries,
                            true,
                            -1,
                            this.searchKeyType,
                            newNode.address
                    );
                    BTreeNode newRoot = bm.readBTreeNode(newHeadPage);
                    //based on my observations, key should be last (max) value of this page
                    Object maxKey = keys.get((int)Math.ceil(keys.size()/2.0 -1));
                    this.lastPoint = IndexEntries.get(maxKey);
                    IndexEntries.remove(maxKey);
                    newRoot.IndexEntries.put(maxKey, this.address);
                    //update our parents
                    myParent = newHeadPage;
                    newNode.myParent = newHeadPage;
                }





            } catch (IOException e) {
                lastUsed = Instant.now();
                throw new RuntimeException(e);
            }

        }   //table is too small
        else if (( myParent != -1 ) && ( (!internal && count < Math.ceil((numEntries -1)/2.0) ||
                (internal && count < Math.ceil(numEntries/2.0))))) {
            Logger.log("Tried borrow on page " + address + " But not implemented yet");
            //logic for merge/borrow
            //do we need this? if only inserting
            //My thought is no.
        }
        else{
            //log because can it hurt to?
            Logger.log("Update page " + address + ": Update called but page was perfectly okay!");

        }
        //note that we accessed this page and change it's time
        lastUsed = Instant.now();
    }

}
