package DMLParser;

import Common.Command;
import Catalog.Catalog;
import Common.Logger;
import Common.Page;
import StorageManager.StorageManager;

import java.sql.SQLSyntaxErrorException;

public class Select implements Command{

    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException{
        // for right now select command is simple
        // SELECT * FROM <table>;

        if (command.length < 4){
            throw new SQLSyntaxErrorException(
                    "Invalid select structure: SELECT * FROM <table>;"
            );
        }

        if (!command[0].equals("select") || !command[1].equals("*") || !command[2].equals("from")){
            throw new SQLSyntaxErrorException(
                    "Invalid select structure: SELECT * FROM <table>;"
            );
        }

        String table = command[3];

        // TODO: verify that table exists
        Catalog cat = Catalog.getInstance();
        if (!cat.tableExists(table)){
            throw new SQLSyntaxErrorException(
                    "Table does not exists"
            );
        }


        // TODO: once implemented run the actual select command
        Logger.log("Running select * on table " + table);
        StorageManager store = StorageManager.getStorageManager();
        try {
            Page currentPage = store.selectFirstPage(table);
            while (true){
                for (int i = 0; i < currentPage.getNumRows(); i++){
                    System.out.println(currentPage.getRecord(i).toString());
                }
                if (currentPage.getNextPage() == -1){
                    break;
                }
                currentPage = store.select(currentPage.getNextPage(), table);
            }

        } catch (Exception e) {
            throw new SQLSyntaxErrorException("Error getting pages");
        }

        return true;
    }

}
