package StorageManager;

import AttributeInfo.Attribute;
import AttributeInfo.AttributeDefinition;
import Common.Page;
import Catalog.TableSchema;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import Catalog.Catalog;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static AttributeInfo.AttributeTypeEnum.VARCHAR;

public class BufferManager {
    private final Map<Integer, Page> bufferPages;
    private static BufferManager bufferManager;
    private int bufferSize;
    private final String dbLocation;

    public static void init(int bufferSize, String dbLocation) {
        if (bufferManager == null) {
            bufferManager = new BufferManager(bufferSize, dbLocation);
        }
    }

    public static BufferManager getInstance() {
        if (bufferManager == null) {
            throw new IllegalStateException("Buffer manager not initialized");
        }
        return bufferManager;
    }

    private BufferManager(int bufferSize, String dbLocation) {
        this.bufferPages  = new HashMap<>();
        this.bufferSize = bufferSize;
        this.dbLocation = dbLocation;
    }

    public void newPage(int Address, String tableName) throws IOException {
        Catalog catalog = Catalog.getInstance();
        Page newPage = new Page(0, Address, -1, Address+(Integer.BYTES*4), Address+ catalog.getPageSize(), true, tableName);
        //adds new page to bufferpages
        addPageToBuffer(newPage);
        newPage.SetModified(true);
    }

    public void dropTable(String tableName) throws Exception {
        Catalog catalog = Catalog.getInstance();
        int pageAddress = catalog.getAddressOfPage(tableName);
        //set all fields to blank and set modified to true
        Page page = this.bufferPages.get(pageAddress);
        if (page == null) {
            if (this.bufferPages.size()+1 > this.bufferSize) {
                removeLRUPage();
            }
            page = readPage(pageAddress, tableName);
        }
        page.setTableName(null);
        page.SetModified(true);
        //while there is a next page set it's tableName to null signifying empty
        while (page.getNextPage() != -1) {
            pageAddress = page.getNextPage();
            page = this.bufferPages.get(pageAddress);
            if (page == null) {
                if (this.bufferPages.size()+1 > this.bufferSize) {
                    removeLRUPage();
                }
                page = readPage(pageAddress, tableName);
            }
            page.setTableName(null);
            page.SetModified(true);
        }
    }

    /**
     * The select operation simply gets a page containing a table based on the provided address and table name and returns it
     * @param address The address of the page
     * @param tableName The name of the table
     * @return a page corresponding to the table and address
     */
    public Page select(int address, String tableName) throws Exception {
        Catalog catalog = Catalog.getInstance();
        if(!catalog.tableExists(tableName)){
            throw new Exception("Table " + tableName + " does not exist");
        }
        Page page = readPage(address, tableName);
        page.updateLastUsed();
        return page;
    }

    public void insert(String tableName, List<List<Object>> rows) throws Exception {
        Catalog catalog = Catalog.getInstance();
        TableSchema table = catalog.getTable(tableName);

        if (table == null) {
            throw new Exception("Table does not exist: " + tableName);
        }

        int pageAddress = catalog.getAddressOfPage(tableName);
        Page currentPage = select(pageAddress, tableName);

        for (List<Object> row : rows) {
            // Convert List<Object> to ArrayList<Object>
            ArrayList<Object> record = new ArrayList<>(row);

            // Calculate the size needed for this record (data + directory overhead)
            int recordSize = calculateRecordSize(table, record);
            int directoryOverhead = Integer.BYTES * 2; // offset and length in directory
            int totalRecordSize = recordSize + directoryOverhead;
            int availableSpace = currentPage.getFreeSpaceEnd() - currentPage.getFreeSpaceStart();

            // If not enough space, we need a new page
            if (availableSpace < totalRecordSize) {
                // Write current page before moving to a new one
                if (currentPage.getModified()) {
                    writePage(currentPage);
                    currentPage.SetModified(false);
                }

                // Get a new page
                int newPageAddress;
                if (catalog.hasFreePages()) {
                    newPageAddress = catalog.getFirstFreePage();
                    catalog.removeFirstFreePage();
                } else {
                    // Allocate a new page at the end
                    newPageAddress = currentPage.getPageAddress() + catalog.getPageSize();
                }

                // Mark current page as having a next page
                currentPage.setNextPage(newPageAddress);
                currentPage.SetModified(true);
                writePage(currentPage);
                currentPage.SetModified(false);

                // Create the new page
                newPage(newPageAddress, tableName);
                currentPage = select(newPageAddress, tableName);
            }

            // Add record to current page
            currentPage.addRecord(record);
            currentPage.setNumRows(currentPage.getNumRows() + 1);

            // Update free space pointers
            //currentPage.setFreeSpaceStart(currentPage.getFreeSpaceStart() + (Integer.BYTES * 2)); // for offset and length
            //currentPage.setFreeSpaceEnd(currentPage.getFreeSpaceEnd() - recordSize);
            currentPage.SetModified(true);
            currentPage.updateLastUsed();
        }

        // Write the final page
        if (currentPage.getModified()) {
            writePage(currentPage);
            currentPage.SetModified(false);
        }

        // Also flush all buffered pages
        //get rid of when shutdown created
        flushAllPages();
    }

    /**
     * Calculate the size needed to store a record on disk
     */
    private int calculateRecordSize(TableSchema table, ArrayList<Object> record) {
        int size = Integer.BYTES; // for null bit array
        List<Attribute> attributes = table.getAttributes();

        for (int i = 0; i < record.size(); i++) {
            if (record.get(i) != null) {
                AttributeDefinition def = attributes.get(i).getDefinition();
                if (def.getType() == VARCHAR) {
                    size += (Integer.BYTES * 2); // offset and length
                    size += record.get(i).toString().length(); // actual string data
                } else {
                    size += def.getByteSize();
                }
            }
        }

        return size;
    }

    /**
     * Flush all modified pages to disk
     */
    public void flushAllPages() throws IOException {
        for (Page page : bufferPages.values()) {
            if (page.getModified()) {
                writePage(page);
                page.SetModified(false);
            }
        }
        this.bufferPages.clear();
    }

    /**
     * Finds the address of the least recently used page and returns it
     * @return the address of the least recently used page
     */
    private Integer getLeastRecentlyUsedPage(){
        Integer leastRecentlyUsedPage = null;
        Instant leastRecentlyUsedTime = null;
        for (Integer address : this.bufferPages.keySet()) {
            Page page = this.bufferPages.get(address);
            if (leastRecentlyUsedPage == null) {
                leastRecentlyUsedTime = page.getLastUsed();
                leastRecentlyUsedPage = address;
            } else{
                if (leastRecentlyUsedTime.isAfter(page.getLastUsed()))
                {
                    leastRecentlyUsedTime = page.getLastUsed();
                    leastRecentlyUsedPage = address;
                }
            }
        }
        return leastRecentlyUsedPage;
    }

    /**
     * Removes the least recently used page from the buffer
     */
    private void removeLRUPage() throws IOException {
        if (bufferPages.isEmpty()) {
            return; // Nothing to remove
        }
        Integer lruPage = getLeastRecentlyUsedPage();
        Page page = bufferPages.get(lruPage);
        if (page == null) {
            return; // Page already removed
        }
        this.bufferPages.remove(lruPage);
        //writes page after removal if modified
        if (page.getModified()) {
            writePage(page);
        }
    }

    /**
     * Adds a page to the buffer. If the page doesn't fit in the buffer the least recently used page is removed
     * from the buffer and the new page is added
     * @param page the page to add to the buffer
     */
    private void addPageToBuffer(Page page) throws IOException {
        //if buffer page will fit in buffer add it, otherwise remove the last used item and add this page
        if (this.bufferSize > 0 && this.bufferPages.size() >= this.bufferSize) {
            removeLRUPage();
        }
        if (this.bufferSize > 0) {
            this.bufferPages.put(page.getPageAddress(), page);
        }
        page.updateLastUsed();
    }

    private void writePage(Page page) throws IOException {
        try (RandomAccessFile currentPage = new RandomAccessFile(dbLocation, "rw")){
            currentPage.seek(page.getPageAddress());
            Catalog catalog = Catalog.getInstance();
            if (page.getTableName() == null) {
                for(int i = 0; i<catalog.getPageSize(); i++){
                    currentPage.write((byte)0);
                }
                return; // Exit early after writing empty page
            }

            // Write page header (4 integers = 16 bytes)
            currentPage.writeInt(page.getNumRows());
            currentPage.writeInt(page.getFreeSpaceStart());
            currentPage.writeInt(page.getFreeSpaceEnd());
            currentPage.writeInt(page.getNextPage());

            TableSchema table = catalog.getTable(page.getTableName());
            List<Attribute> attributes = table.getAttributes();

            int end = page.getPageAddress()+catalog.getPageSize();
            //loop for every record
            for (int i = 0; i < page.getNumRows(); i++) {
                ArrayList<Object> record = page.getRecord(i);
                int recordLength = 0;
                int nullBitArray = 0;
                long fixedEnd = 0;
                // Calculate null bit array and fixed record length
                for (int j = 0; j<record.size(); j++) {
                    if (record.get(j) == null) {
                        nullBitArray += (int)Math.pow(2,j);
                    } else {
                        if (attributes.get(j).getDefinition().getType() == VARCHAR) {
                            // VARCHAR: pointer + length in the record (actual data stored separately)
                            recordLength += Integer.BYTES * 2 + record.get(j).toString().getBytes(StandardCharsets.UTF_8).length;
                            fixedEnd += Integer.BYTES * 2;
                        } else {
                            recordLength += attributes.get(j).getDefinition().getByteSize();
                            fixedEnd += attributes.get(j).getDefinition().getByteSize();
                        }
                    }
                }
                // Add size of null bit array
                recordLength += Integer.BYTES;
                fixedEnd     += Integer.BYTES;

                // Save directory position and write offset and length
                end = end-recordLength;
                fixedEnd    += end;
                currentPage.writeInt(end);
                currentPage.writeInt(recordLength);
                long start = currentPage.getFilePointer();

                // Seek to where record data will be written
                currentPage.seek(end);

                // Write null bit array
                currentPage.writeInt(nullBitArray);

                // Write record data
                for (int j = 0; j<record.size(); j++) {
                    int bit = (int)Math.pow(2,j);
                    if ((nullBitArray & bit) == 0) {
                        switch (attributes.get(j).getDefinition().getType()) {
                            case INTEGER:
                                currentPage.writeInt((int) record.get(j));
                                break;
                            case DOUBLE:
                                currentPage.writeDouble((double) record.get(j));
                                break;
                            case BOOLEAN:
                                currentPage.write((byte) ((boolean) record.get(j) ? 1 : 0));
                                break;
                            case CHAR:
                                currentPage.write(((String) record.get(j)).getBytes(StandardCharsets.UTF_8));
                                break;
                            case VARCHAR:
                                currentPage.writeInt((int)fixedEnd);
                                currentPage.writeInt(((String) record.get(j)).getBytes(StandardCharsets.UTF_8).length);
                                long currentLoc = currentPage.getFilePointer();
                                currentPage.seek(fixedEnd);
                                currentPage.write(((String)record.get(j)).getBytes());
                                fixedEnd = currentPage.getFilePointer();
                                currentPage.seek(currentLoc);
                                break;
                        }
                    }
                }
                // Return to start
                currentPage.seek(start);
            }
        }
    }

    private Page readPage(int pageAddress, String tableName) throws IOException{
        if (this.bufferPages.containsKey(pageAddress)) {
            return this.bufferPages.get(pageAddress);
        }
        try (RandomAccessFile currentPage = new RandomAccessFile(dbLocation, "r")){
            currentPage.seek(pageAddress);
            Catalog catalog = Catalog.getInstance();
            //writes number of entries, start and end
            int numRows =  currentPage.readInt();
            int freeSpaceStart = currentPage.readInt();
            int freeSpaceEnd = currentPage.readInt();
            int nextPage = currentPage.readInt();
            Page page = new Page(numRows, pageAddress,nextPage, freeSpaceStart, freeSpaceEnd, false, tableName);
            TableSchema table = catalog.getTable(tableName);
            List<Attribute> attributes = table.getAttributes();
            //loop for every record
            int end = freeSpaceEnd;
            for (int i = 0; i < numRows; i++) {
                int recordStart = currentPage.readInt();
                int recordLength = currentPage.readInt();
                long start = currentPage.getFilePointer();
                ArrayList<Object> record = new ArrayList<Object>();
                currentPage.seek(recordStart);
                int nullBitArray = currentPage.readInt();
                //loop through and read and save record data.
                for (int j = 0; j<table.getAttributes().size(); j++) {
                    int bit = (int)Math.pow(2,j);
                    // if not null read it in
                    if ((nullBitArray & bit) == 0) {
                        switch (attributes.get(j).getDefinition().getType()) {
                            case INTEGER:
                                record.add(currentPage.readInt());
                                break;
                            case DOUBLE:
                                record.add(currentPage.readDouble());
                                break;
                            case BOOLEAN:
                                record.add(currentPage.read() == 1 ? true : false);
                                break;
                            case CHAR:
                                byte[] b = new byte[attributes.get(j).getDefinition().getByteSize()];
                                currentPage.readFully(b);
                                record.add(new String(b, StandardCharsets.UTF_8));
                                break;
                            case VARCHAR:
                                // Read pointer and length from record
                                int varcharPointer = currentPage.readInt();
                                int varcharLength = currentPage.readInt();

                                // Save current position
                                long currentPos = currentPage.getFilePointer();

                                // Seek to VARCHAR data at end of page
                                currentPage.seek(varcharPointer);
                                byte[] varchar = new byte[varcharLength];
                                currentPage.readFully(varchar);
                                record.add(new String(varchar, StandardCharsets.UTF_8));

                                // Return to record position
                                currentPage.seek(currentPos);
                                break;
                        }
                    } else {
                        // Null value - add null to maintain correct indexing
                        record.add(null);
                    }
                }
                page.addRecord(record);
                currentPage.seek(start);
            }
            addPageToBuffer(page);
            return page;
        }
    }

// todo review below comment

// I don't think we need this, changed the way we drop attributes but didn't want to delete it yet
//    public void DropAttributes(TableSchema table, ArrayList<String> attributes) throws IOException {
//        Page page = this.bufferPages.get(table.getRootPageID());
//        if (page == null) {
//            page = readPage(table.getRootPageID(), table.getTableName());
//        }
//        Catalog catalog = Catalog.getInstance();
//        for(int i = 0; i < attributes.size(); i++) {
//            List<Attribute> previousAttributes= table.getAttributes();
//            int index = 0;
//            for (int j = 0; j < previousAttributes.size(); j++) {
//                if (previousAttributes.get(j).getName().equalsIgnoreCase(attributes.get(i))) {
//                    index = j;
//                }
//            }
//            table.dropAttribute(attributes.get(i));
//            page.removeAttributeFromRecords(index);
//        }
//        page.SetModified(true);
//        page.updateLastUsed();
//    }

    public void AddAttributes(){
        //recompute length
        //change freespaceend
        //if freespaceend <= freespacestart split page
    }

    //use this for writing all pages on shutdown
    public void writePages() throws IOException {
        for (Integer address : this.bufferPages.keySet()) {
            Page page = this.bufferPages.get(address);
            if (page.getModified()) {
                writePage(page);
            }
        }
        bufferPages.clear();
    }
}