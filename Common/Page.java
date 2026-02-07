
public class Page {
    private static Page page;
    private int index;
    private int numRows;
    private int address;
    private int nextPage;
    private int freeSpaceStart;
    private int freeSpaceEnd;
    private Map<int, int> records;

    public void createPage(int index, int numRows, int address, int nextPage, int freeSpaceStart, int freeSpaceEnd) {
        this.index = index;
        this.numRows = numRows;
        this.address = address;
        this.nextPage = nextPage;
        this.freeSpaceStart = freeSpaceStart;
        this.freeSpaceEnd = freeSpaceEnd;
    }

    public int getNextPage(){
        return nextPage;
    }

    public void addRecord(int recordAddress, int length){
        records.put(recordAddress, length);
    }

}