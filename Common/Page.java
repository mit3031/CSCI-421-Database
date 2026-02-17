package Common;

import java.time.Instant;
import java.util.*;

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
    }

    public void updateLastUsed() {
        this.lastUsed = Instant.now();
    }
}