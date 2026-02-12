import Common.Page;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import Catalog.Catalog;
import java.util.HashMap;
import java.util.Map;

public class BufferManager {
    private final Map<Integer, Page> bufferPages;
    private static BufferManager bufferManager;

    public static void createBufferManager() {

        this.bufferManager = new BufferManager();
        this.bufferPages  = new HashMap<>();
    }

    public void newPage(int Address) {
        Catalog catalog = Catalog.getInstance();
        Page newPage = new Page(catalog.getNumTables()+1, 0, Address, null, Address, Address+ catalog.getPageSize());
        this.bufferPages.put(Address,newPage);
    }

    public void dropTable(String tableName) throws Exception {
        Catalog catalog = Catalog.getInstance();
        int pageAddress = catalog.getAddressOfPage(tableName);
        //add read from page if page not in buffer
        Page page = this.bufferPages.get(pageAddress);
        if (page == null) {
            page = readPage(pageAddress);
        }
        //while nextpage not null get rid of page in bufferpages
        while (page.getNextPage() != -1) {
            this.bufferPages.remove(pageAddress);
            //page = page.getNextPage();
        }
    }

    public void writePage(Page page) throws IOException {
        FileOutputStream fos = new FileOutputStream(catalogPath);
        DataOutputStream out = new DataOutputStream(fos);

        //writes number of entries

        out.writeInt(page.getNumRows());

        //write start of free space
        out.writeInt(page.getFreeSpaceEnd());

        //write length and location of records
        for (int i = 0; i < page.getNumRows(); i++) {
//            out.writeInt(page.)
        }
    }

    public Page readPage(int pageAddress){

    }
}