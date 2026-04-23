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
     * Finds the page that a given key should be inserted into, recursively calls itself until it returns a page address
     * @param searchKey the key we are trying to insert
     * @return the page to insert into
     * @Author Logan Maleady lpm5781
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
     * Inserts a primary key into the b+ tree and finds the address of the page the primary key should be inserted into
     * @param searchKey the primary key trying to be inserted into the database and the tree
     * @return the address of the page search key should be inserted into
     * @Author Logan Maleady
     */
    public int insertIntoBTree(Object searchKey){
        BufferManager bufferManager = BufferManager.getInstance();

        try{
            for (Object nodeSearchKey : this.IndexEntries.keySet()){
                int searchKeyCompare = ((Comparable)searchKey).compareTo(nodeSearchKey);
                if(searchKeyCompare < 0 && !this.internal){
                    Integer pageAddress = this.IndexEntries.get(nodeSearchKey);
                    this.IndexEntries.put(searchKey, pageAddress);
                    update();
                    return pageAddress;
                } else if (searchKeyCompare < 0){
                    return bufferManager.readBTreeNode(this.IndexEntries.get(nodeSearchKey)).insertIntoBTree(searchKey);
                }

            }
            if(!this.internal){
                this.IndexEntries.put(searchKey, this.lastPoint);
                this.modified = true;
                update();
                return this.lastPoint;
            } else{
                return bufferManager.readBTreeNode(this.lastPoint).insertIntoBTree(searchKey);
            }

        }catch(IOException e){
            Logger.log("Error while attempting to readBTreeNode");
            lastUsed = Instant.now();
            throw new RuntimeException(e);
        }
    }

    /**
     * Used to insert a search key with a specific page address into the b+ tree
     * @param searchKey the search key to insert into the tree
     * @param pageAddress the address to insert with the key
     */
    public void insertIntoBTree(Object searchKey, Integer pageAddress){
        BufferManager bufferManager = BufferManager.getInstance();

        try{
            for (Object nodeSearchKey : this.IndexEntries.keySet()){
                int searchKeyCompare = ((Comparable)searchKey).compareTo(nodeSearchKey);
                if(searchKeyCompare < 0 && !this.internal){
                    this.IndexEntries.put(searchKey, pageAddress);
                    update();
                } else if (searchKeyCompare < 0){
                    bufferManager.readBTreeNode(this.IndexEntries.get(nodeSearchKey)).insertIntoBTree(searchKey, pageAddress);
                }

            }
            if(this.internal){
                bufferManager.readBTreeNode(this.lastPoint).insertIntoBTree(searchKey, pageAddress);
            }
        }catch(IOException e){
            Logger.log("Error while attempting to readBTreeNode");
            lastUsed = Instant.now();
            throw new RuntimeException(e);
        }
    }

    /**
     * Replace the value of the current search key with a pointer to its new page. Should be used when a page splits.
     * This is a basic implementation that could probably be optimized for doing multiple keys at once if it causes a big
     * performance issue we can change it
     * @param searchKey the key that needs to be changed
     * @param pageAddress the address to change to
     * @Author Logan Maleady
     */
    public void updateSearchKeysPage(Object searchKey, Integer pageAddress){
        BufferManager bufferManager = BufferManager.getInstance();

        try{
            boolean replaced = false;
            for (Object nodeSearchKey : this.IndexEntries.keySet()){
                int searchKeyCompare = ((Comparable)searchKey).compareTo(nodeSearchKey);
                if(!this.internal){
                    //Variable keyExists is only used for debugging
                    boolean keyExists = this.IndexEntries.containsKey(searchKey);
                    //variable is only used for debugging, treeMap.replace() returns null if the key didn't exist or if the value being replaced was null
                    Object successfulReplace = this.IndexEntries.replace(searchKey, pageAddress);
                    replaced = true;
                    if(keyExists && successfulReplace == null){
                        Logger.log("Search key was not replaced as it does not exist");
                    }
                    break;
                } else if (searchKeyCompare < 0){
                    bufferManager.readBTreeNode(this.IndexEntries.get(nodeSearchKey)).updateSearchKeysPage(searchKey, pageAddress);
                }

            }
            if(!this.internal && !replaced){
                boolean keyExists = this.IndexEntries.containsKey(searchKey);
                //variable is only used for debugging, treeMap.replace() returns null if the key didn't exist or if the value being replaced was null
                Object successfulReplace = this.IndexEntries.replace(searchKey, pageAddress);
                if(keyExists && successfulReplace == null){
                    Logger.log("Search key was not replaced as it does not exist");
                }
            } else{
                bufferManager.readBTreeNode(this.lastPoint).updateSearchKeysPage(searchKey, pageAddress);
            }

        }catch(IOException e){
            Logger.log("Error while attempting to readBTreeNode");
            lastUsed = Instant.now();
            throw new RuntimeException(e);
        }
    }

    /**
     * This method checks if a search key is unique. Recursively calls itself to navigate the tree
     * Note: Not sure this is necessary at all, there theoretically shouldn't be a case where you just need to know if it is unique and not also insert it into the tree but maybe
     * @param searchKey the key to check if unique
     * @return true if unique, false otherwise\
     * @Author Logan Maleady
     */
    public boolean checkIfUnique(Object searchKey){
        BufferManager bufferManager = BufferManager.getInstance();

        try{
            for(Object nodeSearchKey : this.IndexEntries.keySet()){
                int searchKeyCompare = ((Comparable)searchKey).compareTo(nodeSearchKey);
                if(searchKeyCompare < 0 && !this.internal){
                    return true;
                } else if (searchKeyCompare < 0){
                    return bufferManager.readBTreeNode(this.IndexEntries.get(nodeSearchKey)).checkIfUnique(searchKey);
                } else if (searchKeyCompare == 0){
                    return false;
                }


            }
            if(!this.internal){
                return true;
            } else{
                return bufferManager.readBTreeNode(this.lastPoint).checkIfUnique(searchKey);
            }
        } catch(IOException e){
            Logger.log("Error while attempting to readBTreeNode");
            lastUsed = Instant.now();
            throw new RuntimeException(e);
        }
    }

    /**
     * This method is used to determine if a search key with the Unique param can be inserted. This will also add that
     * search key to the tree for future references.
     * @param searchKey the value that has a unique constraint and is currently trying to be inserted
     * @return True if the search key is unique, False if it is not
     * @Author Logan Maleady lpm5781
     */
    public boolean insertIntoUnqiueTree(Object searchKey){
        BufferManager bufferManager = BufferManager.getInstance();
        try{
            // loop through every search key in this node to follow the tree down to where the search key we are trying to insert goes
            for(Object nodeSearchKey : this.IndexEntries.keySet()){
                int searchKeyCompare = ((Comparable)searchKey).compareTo(nodeSearchKey);
                
                // FIRST check if the search key is equal to the current search key - if so, it's not unique
                if (searchKeyCompare == 0){
                    return false;
                }
                // if the search key is less than the current search key in the node
                else if (searchKeyCompare < 0){
                    if(!this.internal){
                        this.IndexEntries.put(searchKey, -1);
                        update();
                        return true;
                    } else {
                        // if the node is internal, go to the left of that node
                        return bufferManager.readBTreeNode(this.IndexEntries.get(nodeSearchKey)).insertIntoUnqiueTree(searchKey);
                    }
                }
            }

            if(!this.internal){
                // todo Not sure if this is necessary it might get caught in the loop so this condition will never hit but for safety it is here
                this.IndexEntries.put(searchKey, -1);
                update();
                return true;
            } else{
                return bufferManager.readBTreeNode(this.lastPoint).insertIntoUnqiueTree(searchKey);
            }
        } catch(IOException e){
            Logger.log("Error while attempting to readBTreeNode");
            lastUsed = Instant.now();
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes a value from the leafs a unique tree
     * @param searchKey the key that we want to delete
     * @return true if deleted, false if search key isn't in the tree
     * @Author Logan Maleady
     */
    public boolean deleteFromUnqiueTree(Object searchKey){
        BufferManager bufferManager = BufferManager.getInstance();
        try{
            // loop through every search key in this node to follow the tree down to where the search key we are trying to delete is
            for(Object nodeSearchKey : this.IndexEntries.keySet()){
                int searchKeyCompare = ((Comparable)searchKey).compareTo(nodeSearchKey);
                // if the node is a leaf we can delete the search key
                if(!this.internal){
                    if(this.IndexEntries.containsKey(searchKey)){
                        this.deleteIndex(searchKey);
                        // delete successful
                        return true;
                    } else {
                        return false;
                    }

                    // if the search key is less than the current search key in the node then go to the left of that node
                } else if (searchKeyCompare < 0){
                    return bufferManager.readBTreeNode(this.IndexEntries.get(nodeSearchKey)).deleteFromUnqiueTree(searchKey);
                    // if the search key is equal to the current search key in the node then it is not unique and this should return false
                }
                // if the search key is larger than the current search key in the node we move on to check the next
            }

            // Getting out of the loop means all search keys in the node are smaller than this search key so either
            // go down the right of the tree For if it's a leaf delete it

            if(!this.internal){
                // todo Not sure if this is necessary it should get caught in the loop so this condition will never hit but for safety it is here
                if(this.IndexEntries.containsKey(searchKey)){
                    this.deleteIndex(searchKey);
                    // delete successful
                    return true;
                } else {
                    return false;
                }
            } else{
                return bufferManager.readBTreeNode(this.lastPoint).deleteFromUnqiueTree(searchKey);
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
                this.modified = true;
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
                newNode.modified = true;
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
                    newRoot.modified = true;
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
