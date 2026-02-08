
public class Page {
    private int numRows;
    private int address;
    private int nextPage;
    private int freeSpaceStart;
    private int freeSpaceEnd;
    private Map<int, int> records;

    public Page(int numRows, int address, int nextPage, int freeSpaceStart, int freeSpaceEnd) {
        this.numRows = numRows;
        this.address = address;
        this.nextPage = nextPage;
        this.freeSpaceStart = freeSpaceStart;
        this.freeSpaceEnd = freeSpaceEnd;
    }

    public int getNextPage(){
        return this.nextPage;
    }

    public void addRecord(int recordAddress, int length){
        this.records.put(recordAddress, length);
    }

}