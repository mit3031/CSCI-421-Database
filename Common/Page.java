package Common;

import java.util.HashMap;
import java.util.Map;

public class Page {
    private int numRows;
    private int address;
    private int nextPage;
    private int freeSpaceStart;
    private int freeSpaceEnd;
    private Map<Integer, Integer> records;

    public Page(int numRows, int address, int nextPage, int freeSpaceStart, int freeSpaceEnd) {
        this.numRows = numRows;
        this.address = address;
        // nextPage of -1 means no next page
        this.nextPage = nextPage;
        this.freeSpaceStart = freeSpaceStart;
        this.freeSpaceEnd = freeSpaceEnd;
        records = new HashMap<Integer, Integer>();
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

    public void addRecord(int recordAddress, int length){
        this.records.put(recordAddress, length);
    }

}