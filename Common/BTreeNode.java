package Common;
import AttributeInfo.AttributeTypeEnum;
import Catalog.Catalog;
import Catalog.BTreeSchema;
import StorageManager.StorageManager;
import StorageManager.BufferManager;
import Catalog.TableSchema;

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
    private String attributeName;
    private String tableName;

    public BTreeNode(int numEntries, int address, boolean modified, boolean internal, Integer myParent, AttributeTypeEnum searchKeyType, Integer lastPoint, String attributeName, String tableName){
        this.numEntries = numEntries;
        this.address = address;
        this.modified = modified;
        this.IndexEntries = new TreeMap<>(new KeyCompare());
        this.lastUsed = Instant.now();
        this.internal = internal;
        this.myParent = myParent;
        this.searchKeyType = searchKeyType;
        this.lastPoint = lastPoint;
        this.attributeName = attributeName;
        this.tableName = tableName;
    }

    public int getPageAddress(){ updateLastUsed();return this.address;}
    public void SetModified(boolean modified){ updateLastUsed();this.modified = modified;}
    public boolean getModified(){ updateLastUsed();return this.modified;}
    public Instant getLastUsed(){ updateLastUsed();return this.lastUsed;}
            public void updateLastUsed(){this.lastUsed = Instant.now();}
    public Map<Object, Integer> getIndexEntries(){return this.IndexEntries;}
    public AttributeTypeEnum getSearchKeyType(){ updateLastUsed();return this.searchKeyType;}
    public int getNumEntries(){ updateLastUsed();return this.numEntries;}
    public Integer getMyParent(){ updateLastUsed();return this.myParent;}
    public Integer getLastPoint(){ updateLastUsed();return this.lastPoint;}
    public String getAttributeName(){return this.tableName;}
    public String returnTableName(){return this.attributeName;}
    public void setLastPoint(Integer lastPoint){this.lastPoint = lastPoint; modified = true;}
    //the delete a node entirely set myparent to null
    public void setMyParent(int myParent){ updateLastUsed();this.myParent = myParent; modified = true; updateLastUsed();}
    public boolean isInternal(){ updateLastUsed();return this.internal;}
    public void insertIndex(Object searchKey, int address){
        this.IndexEntries.put(searchKey, address);
        modified = true;
        updateLastUsed();
        //update();
    }
    public void deleteIndex(Object searchKey){
        updateLastUsed();
        this.IndexEntries.remove(searchKey);
        modified = true;
        //update();
    }

    /**
     * Finds the page that a given key should be inserted into, recursively calls itself until it returns a page address
     * @param searchKey the key we are trying to insert
     * @return the page to insert into
     * @Author Logan Maleady lpm5781
     */
    public int findPageToInsert(Object searchKey){
        BufferManager bufferManager = BufferManager.getInstance();
        updateLastUsed();
        try{
            for(Object nodeSearchKey : this.IndexEntries.keySet()){
                int searchKeyCompare = ((Comparable)searchKey).compareTo(nodeSearchKey);
                if(searchKeyCompare < 0 && !this.internal){
                    return this.IndexEntries.get(nodeSearchKey);
                } else if (searchKeyCompare < 0){
                    Logger.log("Trying to get node at address "+ this.IndexEntries.get(nodeSearchKey) + " for search key " + searchKey);
                    return bufferManager.selectBNode(this.IndexEntries.get(nodeSearchKey)).findPageToInsert(searchKey);
                }

                }
            if(!this.internal){
                return this.lastPoint;
            } else{
                Logger.log("Trying to get node at address "+ this.lastPoint + " for search key " + searchKey);
                return bufferManager.selectBNode(this.lastPoint).findPageToInsert(searchKey);
            }
        } catch(IOException e){
            Logger.log("Error while attempting to readBTreeNode when finding page to insert!");
            throw new RuntimeException(e);
        }

    }

    /**
     * Inserts a primary key into the b+ tree and finds the address of the page the primary key should be inserted into
     * @param searchKey the primary key trying to be inserted into the database and the tree
     * @return the address of the page search key should be inserted into
     * @Author Logan Maleady
     */
    public int insertIntoBTree(Object searchKey, String tableName){
        BufferManager bufferManager = BufferManager.getInstance();
        updateLastUsed();
        try{
            for (Object nodeSearchKey : this.IndexEntries.keySet()){
                int searchKeyCompare = ((Comparable)searchKey).compareTo(nodeSearchKey);
                if(searchKeyCompare < 0 && !this.internal){
                    Integer pageAddress = this.IndexEntries.get(nodeSearchKey);
                    this.IndexEntries.put(searchKey, pageAddress);
                    update();
                    return pageAddress;
                } else if (searchKeyCompare < 0){
                    Logger.log("Trying to get node at address "+ this.IndexEntries.get(nodeSearchKey) + " for search key " + searchKey);
                    return bufferManager.selectBNode(this.IndexEntries.get(nodeSearchKey)).insertIntoBTree(searchKey, tableName);
                }

            }
            if(!this.internal){
                if(IndexEntries.isEmpty()){
                    Catalog catalog = Catalog.getInstance();
                    int firstPageofTable = catalog.getAddressOfPage(tableName);
                    this.IndexEntries.put(searchKey, firstPageofTable);
                    this.lastPoint = firstPageofTable;
                    update();
                }
                this.IndexEntries.put(searchKey, this.lastPoint);
                this.modified = true;
                update();
                return this.lastPoint;
            } else{
                Logger.log("Trying to get node at address "+ this.lastPoint + " for search key " + searchKey);
                return bufferManager.selectBNode(this.lastPoint).insertIntoBTree(searchKey, tableName);
            }

        }catch(IOException e){
            Logger.log("Error while attempting to readBTreeNode when inserting into BTree!");
            throw new RuntimeException(e);
        } catch (Exception e) {
            Logger.log("Error while attempting to find first page of table");
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
        updateLastUsed();
        try{
            for (Object nodeSearchKey : this.IndexEntries.keySet()){
                int searchKeyCompare = ((Comparable)searchKey).compareTo(nodeSearchKey);
                if(searchKeyCompare < 0 && !this.internal){
                    this.IndexEntries.put(searchKey, pageAddress);
                    update();
                } else if (searchKeyCompare < 0){
                    Logger.log("Trying to get node at address "+ this.IndexEntries.get(nodeSearchKey) + " for search key " + searchKey);
                    bufferManager.selectBNode(this.IndexEntries.get(nodeSearchKey)).insertIntoBTree(searchKey, pageAddress);
                }

            }
            if(this.internal){
                bufferManager.selectBNode(this.lastPoint).insertIntoBTree(searchKey, pageAddress);
            }
        }catch(IOException e){
            Logger.log("Error while attempting to readBTreeNode when inserting into BTree!");
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
        Catalog cat = Catalog.getInstance();
        Logger.log("First free add: " + cat.getFirstFreeAddress());
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
                    return;
                } else if (searchKeyCompare < 0){
                    Logger.log("Trying to get node at address "+ this.IndexEntries.get(nodeSearchKey) + " for search key " + searchKey);
                    bufferManager.selectBNode(this.IndexEntries.get(nodeSearchKey)).updateSearchKeysPage(searchKey, pageAddress);
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
                Logger.log("Trying to get node at address "+ this.lastPoint + " for search key " + searchKey);
                BTreeNode newBNode = bufferManager.selectBNode(this.lastPoint);
                newBNode.updateSearchKeysPage(searchKey, pageAddress);
            }

        }catch(IOException e){
            Logger.log("Error while attempting to readBTreeNode in updateSearchKeys!");
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
                    Logger.log("Trying to get node at address "+ this.IndexEntries.get(nodeSearchKey) + " for search key " + searchKey);
                    return bufferManager.selectBNode(this.IndexEntries.get(nodeSearchKey)).checkIfUnique(searchKey);
                } else if (searchKeyCompare == 0){
                    return false;
                }


            }
            if(!this.internal){
                return true;
            } else{
                Logger.log("Trying to get node at address "+ this.lastPoint + " for search key " + searchKey);
                return bufferManager.selectBNode(this.lastPoint).checkIfUnique(searchKey);
            }
        } catch(IOException e){
            Logger.log("Error while attempting to readBTreeNode in Unique check!");
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
                        Logger.log("Trying to get node at address "+ this.IndexEntries.get(nodeSearchKey) + " for search key " + searchKey);
                        return bufferManager.selectBNode(this.IndexEntries.get(nodeSearchKey)).insertIntoUnqiueTree(searchKey);
                    }
                }
            }

            if(!this.internal){
                // todo Not sure if this is necessary it might get caught in the loop so this condition will never hit but for safety it is here
                this.IndexEntries.put(searchKey, -1);
                update();
                return true;
            } else{
                Logger.log("Trying to get node at address "+ this.lastPoint + " for search key " + searchKey);
                return bufferManager.selectBNode(this.lastPoint).insertIntoUnqiueTree(searchKey);
            }
        } catch(IOException e){
            Logger.log("Error while attempting to readBTreeNode in Unique Tree Insert!");
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
                    return bufferManager.selectBNode(this.IndexEntries.get(nodeSearchKey)).deleteFromUnqiueTree(searchKey);
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
                return bufferManager.selectBNode(this.lastPoint).deleteFromUnqiueTree(searchKey);
            }
        } catch(IOException e){
            Logger.log("Error while attempting to readBTreeNode in Unique Tree Delete!");
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
                Logger.log("Splitting B+Tree page " + this.address);
                lastUsed = Instant.MAX;
                this.modified = true;
                //need new page slot, get from catalog
                Catalog cat = Catalog.getInstance();
                int newPage = -1;
                if(cat.hasFreePages()){
                    newPage = cat.getFirstFreePage();
                    //also remove it so we don't ruin everything
                    cat.removeFirstFreePage();
                }
                else{
                    newPage = cat.getFirstFreeAddress();
                }
                if(newPage == -1){
                    Logger.log("Bad new Page!");
                    return;
                }


                //create our node
                BufferManager bm = BufferManager.getInstance();
                Logger.log("Splitting: Got Free page");
                bm.newBTreeNode(
                        newPage,
                        this.numEntries,
                        this.internal,
                        this.myParent,
                        this.searchKeyType,
                        this.lastPoint,
                        this.attributeName,
                        this.tableName
                );
                this.lastPoint = newPage;
                BTreeNode newNode = bm.selectBNode(newPage);
                newNode.modified = true;
                Logger.log("Splitting: Created new node to split into at address " + newPage);
                //split records, right half into new node, and remove
                ArrayList keys = new ArrayList<>(this.IndexEntries.keySet());
                Object newNodeMin = keys.get( (int)Math.ceil(keys.size()/2.0));
                for(int i = (int)Math.ceil(keys.size()/2.0); i<keys.size(); i++){
                    //copy 2nd half to new
                    int oldVal = this.IndexEntries.get(keys.get(i));
                    newNode.IndexEntries.put(keys.get(i), oldVal);
                    this.IndexEntries.remove(keys.get(i));
                }
                Logger.log("Added values to new node!");
                //update parents of new children nodes of new node GIVEN INTERNAL
                if(internal) {
                    for (int childIndex : newNode.IndexEntries.values()) {
                        Logger.log("Getting B+Tree Node " + childIndex);
                        BTreeNode child = bm.selectBNode(childIndex);
                        child.setMyParent(newPage);
                    }
                }
                Logger.log("Children of new node updated parent!");
                newNode.updateLastUsed();
                StorageManager sm = StorageManager.getStorageManager();
                //update up tree
                int parentToUpdate = this.myParent;
                Logger.log("Copied entries from this node to new node");
                if(parentToUpdate != -1){
                    Logger.log("Node was not parent, no additional creation necessary");
                    //case where we have a parent
                    BTreeNode parent = bm.selectBNode(parentToUpdate);

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
                        if(parent.lastPoint != this.address){
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
                    int newHeadPage = -1;
                    if(cat.hasFreePages()){
                        newHeadPage = cat.getFirstFreePage();
                        //also remove it so we don't ruin everything
                        cat.removeFirstFreePage();
                    }
                    else{
                        newHeadPage = cat.getFirstFreeAddress();
                    }
                    if (newHeadPage == -1){
                        Logger.log("Bad new page!");
                        return;
                    }

                    bm.newBTreeNode(
                            newHeadPage,
                            this.numEntries,
                            true,
                            -1,
                            this.searchKeyType,
                            newNode.address,
                            this.attributeName,
                            this.tableName
                    );
                    BTreeNode newRoot = bm.selectBNode(newHeadPage);
                    newRoot.modified = true;
                    Logger.log("Created new Root Node!");
                    //based on my observations, key should be last (max) value of this page
                    Object maxKey = keys.get((int)Math.ceil(keys.size()/2.0 -1));
                    this.lastPoint = this.IndexEntries.get(maxKey);
                    this.IndexEntries.remove(maxKey);
                    newRoot.IndexEntries.put(maxKey, this.address);
                    //update our parents
                    this.myParent = newHeadPage;
                    newNode.myParent = newHeadPage;

                    Logger.log("Updating root of page");
                    //update schema to have proper root
                    TableSchema ts = cat.getTable(tableName);
                    BTreeSchema schema = ts.getIndex(attributeName);
                    schema.setRootNodeAddress(newHeadPage);
                    Logger.log("Split with new Layer complete! New Root Node: " + newHeadPage);
                    Logger.log("New head page size: " + newRoot.IndexEntries.size());
                    Logger.log("This page's size: " + IndexEntries.size());
                    Logger.log("Other page's Size: " + newNode.IndexEntries.size());
                    Logger.log("N: " + numEntries);

                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }   //table is too small
        else if (( myParent != -1 ) && ( (!internal && count < Math.ceil((numEntries -1)/2.0) ||
                (internal && count < Math.ceil(numEntries/2.0))))) {
            Logger.log("Tried borrow on page " + address + " But not implemented yet");
            Logger.log("Size of page: " + IndexEntries.size() + ": N for this table: " + numEntries);
            Logger.log("Is internal? " + internal);
            //logic for merge/borrow
            //do we need this? if only inserting
            //My thought is no.
        }
        else{
            //log because can it hurt to?
            Logger.log("Update page " + address + ": Update called but size okay! Num entries: " + IndexEntries.size());

        }

    }

}
