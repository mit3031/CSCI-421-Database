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

        // Check keywords (case-insensitive)
        if (!command[0].equalsIgnoreCase("select") || 
            !command[1].equals("*") || 
            !command[2].equalsIgnoreCase("from")){
            throw new SQLSyntaxErrorException(
                    "Invalid select structure: SELECT * FROM <table>;"
            );
        }

        // Table name - preserve case as provided
        String tableName = command[3].toLowerCase();

        // Verify that table exists
        Catalog cat = Catalog.getInstance();
        if (!cat.tableExists(tableName)){
            throw new SQLSyntaxErrorException(
                    "Table does not exist: " + tableName
            );
        }

        // Run the actual select command
        Logger.log("Running SELECT * FROM " + tableName);
        StorageManager store = StorageManager.getStorageManager();
        try {
            Page currentPage = store.selectFirstPage(tableName);
            
            if (currentPage == null) {
                // Empty table
                Logger.log("Table " + tableName + " is empty");
                return true;
            }
            
            while (true){
                for (int i = 0; i < currentPage.getNumRows(); i++){
                    System.out.println(currentPage.getRecord(i).toString());
                }
                if (currentPage.getNextPage() == -1){
                    break;
                }
                currentPage = store.select(currentPage.getNextPage(), tableName);
            }

        } catch (Exception e) {
            throw new SQLSyntaxErrorException("Error reading from table: " + e.getMessage());
        }

        return true;
    }

}
