package DDLParser;

import AttributeInfo.Attribute;
import Catalog.Catalog;
import Catalog.TableSchema;
import Common.Command;
import Common.Logger;
import Common.Page;
import StorageManager.StorageManager;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;

public class AlterTableDrop implements Command {

    /**
     * Drop an attribute from a table with the following algorithm:
     * Create a new table with a temporary name and without the attribute to be dropped. Add all rows to the new table,
     * removing the old attribute before adding it. Drop the original table. Rename the new table to the original tables name
     * @param command the sequence of commands passed in from the parser
     * @return success of the operation
     * @throws SQLSyntaxErrorException
     */
    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException {
        int TABLE_NAME_INDEX = 2;
        int ATTRIBUTE_NAME_INDEX = 4;
        String TEMP_TABLE_NAME = "$temp";

        Catalog catalog = Catalog.getInstance();
        String tableName = command[TABLE_NAME_INDEX].toLowerCase();
        StorageManager storageManager = StorageManager.getStorageManager();
        String attributeNameToDrop = command[ATTRIBUTE_NAME_INDEX].toLowerCase();

        TableSchema originalTable = catalog.getTable(tableName);
        if (originalTable == null) {
            Logger.log("Table " + tableName + " not found");
            return false;
        }

        // Create a new list of attributes without the dropped attribute
        List<Attribute> oldAttributes = originalTable.getAttributes();
        List<Attribute> newAttributes = new ArrayList<>();
        for (Attribute attribute : oldAttributes) {
            if (!attribute.getName().equalsIgnoreCase(attributeNameToDrop)) {
                newAttributes.add(attribute);
            }
        }

        TableSchema newTable = new TableSchema(TEMP_TABLE_NAME, newAttributes);

        // Add all old rows to new table
        try{
            storageManager.CreateTable(newTable);
            Page currPage = storageManager.selectFirstPage(tableName);
            Integer attributeIndex = currPage.getAttributeIndex(attributeNameToDrop);
            if (attributeIndex == null) {
                throw new SQLSyntaxErrorException("Attribute " + attributeNameToDrop + " not found");
            }
            while(true){
                List<List<Object>> rows = new ArrayList<>();

                // For each row in the page remove the old attribute add it to the collection of new rows for the page
                for (int i = 0; i < currPage.getNumRows(); i++) {
                    List<Object> row = currPage.getRecord(i);
                    row.remove((int) attributeIndex);
                    rows.add(row);
                }

                // insert in batches
                storageManager.insert(TEMP_TABLE_NAME, rows);

                if (currPage.getNextPage() != -1) {
                    currPage = storageManager.select(currPage.getNextPage(), tableName);
                } else {
                    break;
                }
            }

            storageManager.DropTable(originalTable);

            catalog.renameTable(TEMP_TABLE_NAME, tableName);


        } catch (Exception e) {
            throw new SQLSyntaxErrorException(e.getMessage());
        }

        return true;
    }
}
