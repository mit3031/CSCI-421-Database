package DDLParser;

import Catalog.Catalog;
import Common.Command;
import Common.Logger;
import StorageManager.StorageManager;
import Catalog.TableSchema;

import java.sql.SQLSyntaxErrorException;

/**
 * Command responsible for handling the drop of a table
 */
public class DropTable implements Command {
    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException {
        if(command.length != 3){
            Logger.log("Expected 3 words in Drop Table, got " + command.length);
            throw new SQLSyntaxErrorException("Incorrect amount of words given for Dropping table!");
        }
        String tableName = command[2];
        tableName = tableName.substring(tableName.indexOf(";"));

        //TODO call storage manager to delete that table
        try{
            StorageManager sm = StorageManager.getStorageManager();
            Catalog cat = Catalog.getInstance();
            TableSchema table = cat.getTable(tableName);
            if(table == null){
                System.out.println("Table " + tableName + " does not exist!");
                Logger.log("Table " + tableName + " not found in catalog");
                return false;
            }

            sm.DropTable(table);
        } catch (Exception e) {
            Logger.log(e.getMessage());
            throw new RuntimeException(e);
        }
        return true;

    }
}
