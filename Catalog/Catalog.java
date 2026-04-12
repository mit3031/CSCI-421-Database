package Catalog;

import AttributeInfo.*;
import Common.Logger;
import Common.Page;
import StorageManager.StorageManager;

import java.io.*;
import java.util.*;

public class Catalog {

    /*/
    This keeps the catalog as a singleton (aka only one catalog can exist)
     */
    private static Catalog instance;
    // Phase 3 indexing on or off
    private boolean indexingEnabled;

    public static void init(String dbPath, int pageSize, boolean indexingEnabled) {
        if (instance == null) {
            instance = new Catalog(dbPath, pageSize, indexingEnabled);
        }
    }

    public static Catalog getInstance() {
        if (instance == null) {
            throw new IllegalStateException("Catalog not initialized");
        }
        return instance;
    }

    // Only for testing purposes: resets singleton to simulate program restart
    public static void resetForTesting() {
        instance = null;
    }

    /*
    Catalog setup for the actual state of a catalog
     */
    private final Map<String, TableSchema> tables;
    private final String catalogPath;
    private int firstFreeAddress;

    // NOTE: firstFreePage stored here for simplicity
    // May move to StorageManager metadata in later phases.
    private int pageSize;        // Required for DB restart
    private LinkedList<Integer> firstFreePage;   // list of empty pages

    private Catalog(String dbPath, int pageSize, boolean indexingEnabled) {
        this.tables = new HashMap<>();
        this.catalogPath = dbPath + "/catalog.bin";

        // This ensures the directory exists before we try to save/load anything.
        File catalogFile = new File(dbPath);
        catalogFile.mkdirs();
        this.firstFreeAddress = 0;
        this.pageSize = pageSize;      // Default if no file exists
        this.firstFreePage = new LinkedList<Integer>();       // by default empty list of pages
        this.indexingEnabled = indexingEnabled; // Indexing flag
    }

    /*
    Getters and setters for page info
     */
    public boolean getIndexingEnabled() {
        return this.indexingEnabled;
    }

    public void setPageSize(int pageSize) {this.pageSize = pageSize;}

    public int getPageSize(){
        return this.pageSize;
    }

    public int getNumTables() {
        return this.tables.size();
    }

    public int getFirstFreePage() {
        return this.firstFreePage.getFirst();
    }

    public LinkedList<Integer> getAllFreePages() {return this.firstFreePage;}

    public boolean hasFreePages() {
        return !this.firstFreePage.isEmpty();
    }

    public int getFirstFreeAddress() {return firstFreeAddress;}
    public void setFirstFreeAddress(int unusedAddress) {this.firstFreeAddress = unusedAddress;}

    public void addFirstFreePage(int pageAddress) {
        this.firstFreePage.addFirst(pageAddress);
    }

    public void removeFirstFreePage() {
        this.firstFreePage.removeFirst();
    }

    /*
    Public methods below
    /
     */
    public void addTable(TableSchema table) {
        String name = table.getTableName().toLowerCase();
        if (tables.containsKey(name)) {
            throw new RuntimeException("Table already exists: " + name);
        }
        tables.put(name, table);
    }

    public void dropTable(String tableName) {
        tables.remove(tableName.toLowerCase());
    }

    public TableSchema getTable(String tableName) {
        return tables.get(tableName.toLowerCase());
    }


    /**
     * Note: Attribute acts as an extension to attribute defintion, this does not get the actual instance of the
     * attributes, just lets you gather info on them
     * @param tableName: name of the table attributes you want to get info on
     * @return list of the attributes for this table
     */
    public List<Attribute> getAttribute(String tableName){
        return tables.get(tableName.toLowerCase()).getAttributes();
    }

    /**
     * Gets the address of the first page instance
     * @param tableName: name of the table
     * @return the address of the first table
     */
    public int getAddressOfPage(String tableName) throws Exception {
        TableSchema table = tables.get(tableName.toLowerCase());

        if (table == null){
            throw new Exception("Table does not exists yet");
        }

        if (table.getRootPageID() == -1){
            // abort, something is wrong
            throw new Exception("Table does not exist yet");
        }

        return table.getRootPageID();
    }

    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName.toLowerCase());
    }

    public Collection<TableSchema> getAllTables() {
        return tables.values();
    }

    /**
     * Renames a table, first renames the table schema, removes the old table, checks to ensure name
     * in all the pages is the new table name
     * @param oldTableName the old table name
     * @param newTableName the new name of the table
     * @throws Exception if there is an io exception in reading the pages or the table doesn't exist
     */
    public void renameTable(String oldTableName, String newTableName) throws Exception {
        if(getTable(oldTableName) == null){
            System.out.println("Table " + oldTableName + " not found");
        }
        TableSchema table = getTable(oldTableName);
        table.renameTable(newTableName);
        tables.remove(oldTableName);
        tables.put(newTableName.toLowerCase(), table);
        StorageManager storageManager = StorageManager.getStorageManager();
        Page currPage = storageManager.selectFirstPage(newTableName);
        while(true){
            if (!currPage.getTableName().equalsIgnoreCase(newTableName)){
                currPage.setTableName(newTableName);
            }
            if(currPage.getNextPage() != -1){
                currPage = storageManager.select(currPage.getNextPage(), newTableName);
            } else {
                break;
            }
        }

    }

    public String getCatalogPath(){ return this.catalogPath;}

    /**
     * Calculates the order n of a B+ tree based on page size and attribute type
     * n is the maximum number of pointers a node can hold
     * Formula: n = floor(PageSize / (SearchKeySize + PointerSize))
     */
    public int calculateTreeOrder(AttributeDefinition def) {
        int pageSize = this.pageSize;
        int pointerSize = 4;
        int searchKeySize = def.getByteSize();

        int n = pageSize / (searchKeySize + pointerSize);

        return n;
    }
}