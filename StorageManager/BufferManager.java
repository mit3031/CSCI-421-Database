import java.util.ArrayList;
public class BufferManager {
    private ArrayList<Page> bufferPages
    private static BufferManager bufferManager;

    public static void createBufferManager() {

        bufferManager = new BufferManager();
        bufferPages  = new ArrayList<Page>();
    }

    public Page newPage(int Address) {
        Catalog catalog = Catalog.getInstance();
        Page newPage = new Page();
        newPage.createPage(catalog.tables.size()+1, 0, Address, null, Address, Address+catalog.pageSize);
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
}