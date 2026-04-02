package DMLParser;

import Catalog.Catalog;
import Catalog.TableSchema;
import Common.Page;
import Common.Where.IWhereOp;
import StorageManager.StorageManager;

import static Common.Where.BuildTree.buildTree;

public class Delete {
    public static Boolean run(String tableName, String whereClause) throws Exception {
        Catalog catalog = Catalog.getInstance();
        StorageManager storageManager = StorageManager.getStorageManager();
        TableSchema newTable = new TableSchema("$delete", catalog.getTable(tableName).getAttributes());
        storageManager.CreateTable(newTable);

        if (!whereClause.equals("")) {
            IWhereOp whereTree = null;
            try {
                whereTree = buildTree(whereClause, newTable);
            } catch (Exception e) {
                storageManager.DropTable(newTable);
                throw e;
            }
            if (whereTree == null) {
                storageManager.DropTable(newTable);
                return false;
            }
            //find start of old table
            Page page = storageManager.selectFirstPage(tableName);
            int address = catalog.getAddressOfPage("$delete");
            int nextPage = -1;
            while (page != null) {
                for (int i = 0; i < page.getNumRows(); i++) {
                    if (!whereTree.evaluate(page.getRecord(i), newTable)) {
                        address = storageManager.insertSingleRow("$delete", page.getRecord(i), address);
                    }
                }
                nextPage = page.getNextPage();
                if (nextPage != -1) {
                    page = storageManager.select(nextPage, tableName);
                } else {
                    page = null;
                }

            }
        }
        storageManager.DropTable(catalog.getTable(tableName));
        catalog.renameTable("$delete", tableName);
        return true;
    }
}
