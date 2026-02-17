package StorageManager;

import AttributeInfo.Attribute;
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
            throw new IllegalStateException("Catalog not initialized");
        }
        return bufferManager;
    }

    private BufferManager(int bufferSize, String dbLocation) {
        this.bufferPages  = new HashMap<>();
        this.bufferSize = bufferSize;
        this.dbLocation = dbLocation;
    }

    public void newPage(int Address, String tableName) {
        Catalog catalog = Catalog.getInstance();
        Page newPage = new Page(catalog.getNumTables()+1, Address, -1, Address+(Integer.BYTES*3),Address+ catalog.getPageSize(), true, tableName);
        //add check for buffersize if buffersize exceeded get rid of last page used.
        this.bufferPages.put(Address,newPage);
    }

    public void dropTable(String tableName) throws Exception {
        Catalog catalog = Catalog.getInstance();
        int pageAddress = catalog.getAddressOfPage(tableName);
        //add get rid of info in page
        //set all fields to blank and set modified to true
        Page page = this.bufferPages.get(pageAddress);
        if (page == null) {
            //add check for buffersize if buffersize exceeded get rid of last page used.
            page = readPage(pageAddress, tableName);
        }
        //while there is a next page set it's tableName to null signifying empty
        while (page.getNextPage() != -1) {
            page.setTableName(null);
            pageAddress = page.getNextPage();
            page = this.bufferPages.get(pageAddress);
            if (page == null) {
                page = readPage(pageAddress, tableName);
            }
        }
    }

    /**
     * The select operation simply gets a page containing a table based on the provided address and table name and returns it
     * @param address The address of the page
     * @param tableName The name of the table
     * @return a page corresponding to the table and address
     */
    public Page select(int address, String tableName) throws IOException {
        Page page = readPage(address, tableName);
        return page;
    }

    public void insert(String tableName, List<List<Object>> rows){

    }

//    private void addPageToBuffer(Page page) {
//        //if buffer page will fit in buffer add it, otherwise remove the last used item and add this page
//        this.bufferPages.put(page.getPageAddress(), page);
//    }

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
    private void removeLRUPage(){
        Integer lruPage = getLeastRecentlyUsedPage();
        this.bufferPages.remove(lruPage);
    }

    private void writePage(Page page) throws IOException {
        try (RandomAccessFile currentPage = new RandomAccessFile(dbLocation, "rw")){
            currentPage.seek(page.getPageAddress());
            Catalog catalog = Catalog.getInstance();
            if (page.getTableName() == null) {
                for(int i = 0; i<catalog.getPageSize(); i++){
                    currentPage.write((byte)0);
                }
            }
            ByteBuffer byteBuffer = ByteBuffer.allocate(4);
            byteBuffer.putInt(page.getNumRows());
            byteBuffer.putInt(page.getFreeSpaceStart());
            byteBuffer.putInt(page.getFreeSpaceEnd());
            byteBuffer.putInt(page.getNextPage());
            byteBuffer.rewind();
            //writes number of entries, start, end, and nextPage
            currentPage.write(byteBuffer.get());
            currentPage.write(byteBuffer.get());
            currentPage.write(byteBuffer.get());
            currentPage.write(byteBuffer.get());
            TableSchema table = catalog.getTable(page.getTableName());
            List<Attribute> attributes = table.getAttributes();
            //loop for every record
            int end = page.getFreeSpaceEnd();
            for (int i = 0; i < page.getNumRows(); i++) {
                ArrayList<Object> record = page.getRecord(i);
                ByteBuffer recordBuffer = ByteBuffer.allocate(record.size());
                int length = 0;
                int nullBitArray = 0b0000;
                long fixedEnd = 0;
                for (int j = 0; j<record.size(); j++) {
                    if (attributes.get(j).getDefinition().getType() == VARCHAR){
                        length += record.get(j).toString().length()+(Integer.BYTES*2);
                        fixedEnd += (Integer.BYTES*2);
                    } else {
                        length += attributes.get(j).getDefinition().getByteSize();
                        fixedEnd += attributes.get(j).getDefinition().getByteSize();
                    }
                    if (record.get(j) == null) {
                        nullBitArray+= Math.pow(2,j);
                        //null bit array at j gets 1
                    }
                }
                //make null bit array
                recordBuffer.putInt(nullBitArray);
                recordBuffer.rewind();
                end = end - length;
                long start = currentPage.getFilePointer();
                currentPage.seek(end);
                //write null bit array
                currentPage.write(recordBuffer.get());
                fixedEnd += currentPage.getFilePointer();
                //loop through and add record data to byte buffer
                for (int j = 0; j<record.size(); j++) {
                    //handle writing null values
                    switch (attributes.get(j).getDefinition().getType()) {
                        case INTEGER:
                            currentPage.write(ByteBuffer.allocate(Integer.BYTES).putInt((int)record.get(j)).array());
                        case DOUBLE:
                            currentPage.write(ByteBuffer.allocate(Double.BYTES).putDouble((double)record.get(j)).array());
                        case BOOLEAN:
                            currentPage.write((byte)((boolean)record.get(j) ? 1 : 0 ));
                        case CHAR:
                            currentPage.write(((String)record.get(j)).getBytes());
                        case VARCHAR:
                            long currentLoc = currentPage.getFilePointer();
                            currentPage.seek(fixedEnd);
                            currentPage.write(((String)record.get(j)).getBytes());
                            fixedEnd = currentPage.getFilePointer();
                            currentPage.seek(currentLoc);
                    }
                }
                currentPage.seek(start);
                currentPage.write(ByteBuffer.allocate(Integer.BYTES).putInt(end).array());
                currentPage.write(ByteBuffer.allocate(Integer.BYTES).putInt(length).array());
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
                //end = end - length;
                int nullBitArray = currentPage.readInt();
                //loop through and read and save record data.
                for (int j = 0; j<table.getAttributes().size(); j++) {
                    //handle reading null values
                    switch (attributes.get(j).getDefinition().getType()) {
                        case INTEGER:
                            record.add(currentPage.readInt());
                        case DOUBLE:
                            record.add(currentPage.readDouble());
                        case BOOLEAN:
                            record.add(currentPage.read() == 1 ? true : false);
                        case CHAR:
                            byte [] b = new byte [attributes.get(j).getDefinition().getByteSize()];
                            currentPage.readFully(b);
                            record.add(new String(b, StandardCharsets.UTF_8));
                            currentPage.write(((String)record.get(j)).getBytes());
                        case VARCHAR:
                            int beginning = currentPage.readInt();
                            int length = currentPage.readInt();
                            long currentLoc = currentPage.getFilePointer();
                            currentPage.seek(beginning);
                            byte [] varchar = new byte [length];
                            currentPage.readFully(varchar);
                            record.add(new String(varchar, StandardCharsets.UTF_8));
                            currentPage.seek(currentLoc);
                    }
                }
                page.addRecord(record);
                currentPage.seek(start);
            }
//            this.bufferPages.addPageToBuffer(page)
            return page;
        }
    }
}