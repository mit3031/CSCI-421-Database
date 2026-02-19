package Common;

import AttributeInfo.Attribute;
import Catalog.TableSchema;
import Catalog.Catalog;

import java.time.Instant;
import java.util.*;

import static AttributeInfo.AttributeTypeEnum.VARCHAR;

public class Page {
    private int numRows;
    private int address;
    private int nextPage;
    private int freeSpaceStart;
    private int freeSpaceEnd;
    //actual data in each record in arraylist of arraylists
    private ArrayList<ArrayList<Object>> records;
    private Instant lastUsed;
    private boolean modified;
    private String tableName;

    public Page(int numRows, int address, int nextPage, int freeSpaceStart, int freeSpaceEnd, boolean modified, String tableName) {
        this.numRows = numRows;
        this.address = address;
        // nextPage of -1 means no next page
        this.nextPage = nextPage;
        this.freeSpaceStart = freeSpaceStart;
        this.freeSpaceEnd = freeSpaceEnd;
        records = new ArrayList<ArrayList<Object>>();
        this.modified = modified;
        this.tableName = tableName;
        lastUsed = Instant.now();
    }

    public int getNextPage(){
        return this.nextPage;
    }

    public int getNumRows() {
        return numRows;
    }

    public int getFreeSpaceStart(){
        return freeSpaceStart;
    }

    public int getFreeSpaceEnd(){
        return freeSpaceEnd;
    }

    public int getPageAddress(){ return address;}
    public void SetModified(boolean modified){ this.modified = modified;}
    public boolean getModified(){ return this.modified;}
    public String getTableName(){ return this.tableName;}
    public void setTableName(String tableName) {this.tableName = tableName;}

    public ArrayList<Object> getRecord(int index){ return this.records.get(index);}
    public Instant getLastUsed(){
        return lastUsed;
    }

    public void addRecord(ArrayList<Object> recordData){
        this.records.add(recordData);
        this.modified = true;
        Catalog catalog = Catalog.getInstance();
        TableSchema table = catalog.getTable(this.tableName);
        List<Attribute> attributes = table.getAttributes();
        //replace with recordlength function later?
        int length = 0;
        for (int i = 0; i<recordData.size(); i++) {
            if (recordData.get(i) != null) {
                if (attributes.get(i).getDefinition().getType() == VARCHAR) {
                    length += recordData.get(i).toString().length() + (Integer.BYTES * 2);
                } else {
                    length += attributes.get(i).getDefinition().getByteSize();
                }
            }
        }
        length += Integer.BYTES;
        //update freespacestart and freespaceend
        this.freeSpaceStart = this.freeSpaceStart + (Integer.BYTES*2);
        this.freeSpaceEnd -= length;
    }

    public void removeAttributeFromRecords(int index){
        for(int i = 0; i < this.records.size(); i++) {
            this.records.get(i).remove(index);
        }
        Catalog catalog = Catalog.getInstance();
        TableSchema table = catalog.getTable(this.tableName);
        List<Attribute> attributes = table.getAttributes();
        int totalLength = 0;
        for (int i = 0; i < numRows; i++) {
            //replace with recordlength function later?
            int length = 0;
            for (int j = 0; j < this.records.size(); j++) {
                if (this.records.get(j) != null) {
                    if (attributes.get(j).getDefinition().getType() == VARCHAR) {
                        length += this.records.get(j).toString().length() + (Integer.BYTES * 2);
                    } else {
                        length += attributes.get(j).getDefinition().getByteSize();
                    }
                }
            }
            totalLength += length+Integer.BYTES;
        }
        //freespaceend = (pageaddress+pagesize) end of page - length of all records
        this.freeSpaceEnd = (this.address+catalog.getPageSize()) - totalLength;
    }

    public void updateLastUsed() {
        this.lastUsed = Instant.now();
    }
}