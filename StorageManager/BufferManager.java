import java.util.ArrayList;
public class BufferManager {
    private final Map<int, Page> bufferPages;
    private static BufferManager bufferManager;

    public static void createBufferManager() {

        this.bufferManager = new BufferManager();
        this.bufferPages  = new HashMap<>();
    }

    public void newPage(int Address) {
        Catalog catalog = Catalog.getInstance();
        Page newPage = new Page(catalog.getNumTables()+1, 0, Address, null, Address, Address+catalog.pageSize);
        this.bufferPages.put(Address,newPage);
    }

    public void dropTable(String tableName){
        Catalog catalog = Catalog.getInstance();
        int pageAddress = catalog.getAddressOfPage(tableName);
        //add read from page if page not in buffer
        Page page = this.bufferPages.get(pageAddress);
        if page == null {
            page = readPage(pageAddress);
        }
        //while nextpage not null get rid of page in bufferpages
        while (pageAddress.getNextPage() != null) {
            this.bufferPages.remove(pageAddress);
            page = page.getNextPage();
        }
    }

    public void writePage(Page page){
        FileOutputStream fos = new FileOutputStream(catalogPath);
        out = new DataOutputStream(fos);

        //writes number of entries
        out.WriteInt(page.numRows)

        //write start of free space
        out.WriteInt(page.freeSpaceEnd);

        //write length and location of records
        for (int i = 0; i < page.numRows; i++) {
            out.WriteInt(page.)
        }
    }

    public void readPage(Page page){

    }
}