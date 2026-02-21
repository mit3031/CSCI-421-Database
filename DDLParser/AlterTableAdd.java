package DDLParser;

import AttributeInfo.*;
import Catalog.Catalog;
import Catalog.TableSchema;
import Common.Command;
import Common.Logger;
import Common.Page;
import StorageManager.StorageManager;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class to handle running an alter table, in which an attribute is added
 */
public class AlterTableAdd implements Command {

    private static final int MIN_QUERY_LEN = 6;
    private static final int TABLE_NAME_INDEX = 2;
    private static final int NEW_ATT_NAME_INDEX = 4;
    private static final int NEW_ATT_TYPE_INDEX = 5;
    private static final String TEMP_TABLE_NAME = "$temp";

    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException {
        if(command.length <MIN_QUERY_LEN) {
            Logger.log("Command too short for Alter Table Drop! Min. Len " +  MIN_QUERY_LEN + " Got " + command.length);
        }

        String tableName =  command[TABLE_NAME_INDEX].toLowerCase();
        Catalog cat = Catalog.getInstance();
        TableSchema tableSchema = cat.getTable(tableName);
        //we do not have table
        if(tableSchema == null) {
            Logger.log("Table " + tableName + " not found from Catalog!");
            return false;
        }

        String attName = command[NEW_ATT_NAME_INDEX];

        String attType = command[NEW_ATT_TYPE_INDEX];
        if(attType.contains(";")){
            attType = attType.substring(0, attType.indexOf(";"));
        }

        //convert attribute name to lowercase and check if alphanumeric
        attName = attName.toLowerCase();
        if (!attName.matches("^[a-zA-Z0-9]+$")) {
            Logger.log("Invalid attribute name " + attName);
            return false;
        }

        //check that attribute does not already exist for this table
        for(Attribute att : tableSchema.getAttributes()) {
            if(att.getName().equals(attName)) {
                Logger.log("Alter table attribute " + attName + " already exists!");
                return false;
            }
        }
        String defaultValue = null;
        boolean notNull = false;
        //now we can get rest of info about this
        for(int i = 6; i<command.length; i++) {
            if(command[i].contains(";")){
                command[i] = command[i].substring(0, command[i].indexOf(";"));
            }

            if(command[i].equals("NOTNULL")) {
                notNull = true;
            }
            else if(command[i].equals("DEFAULT")) {
                i++;
                if(i<command.length) {
                    defaultValue = command[i];
                    if(defaultValue.contains(";")){
                        defaultValue = defaultValue.substring(0, defaultValue.indexOf(";"));
                    }
                }
                else{
                    Logger.log("Default value not found, reached end of command!");
                }
            }
            else{
                Logger.log("Default specified but not given value");
            }
        }

        if(notNull && defaultValue == null){
            System.out.println("Not null requires a default value when altering a table");
            return false;
        }

        //casted default value so we don't super fail
        Object defCast;

        //define our attribute
        AttributeDefinition def;
        if(attType.equals("INTEGER")){
            def = new IntegerDefinition(AttributeTypeEnum.INTEGER, false, !notNull);
            defCast = (defaultValue!=null) ? Integer.parseInt(defaultValue) : null;
        } else if (attType.equals("DOUBLE")) {
            def = new DoubleDefinition(false, !notNull);
            defCast = (defaultValue!=null) ? Double.parseDouble(defaultValue) : null;
        } else if (attType.equals("BOOLEAN")) {
            def = new BooleanDefinition(false, !notNull);
            defCast = (defaultValue!=null) ? Boolean.parseBoolean(defaultValue) : null;
        } else if (attType.startsWith("CHAR")){
            String lenStr = attType.substring(attType.indexOf("(") + 1, attType.indexOf(")"));
            int len =  Integer.parseInt(lenStr);
            def = new CharDefinition(false, !notNull, len);
            defCast = defaultValue;
        } else if (attType.startsWith("VARCHAR")){
            String lenStr = attType.substring(attType.indexOf("(") + 1, attType.indexOf(")"));
            int len = Integer.parseInt(lenStr);
            def = new CharDefinition(false, !notNull, len);
            defCast = defaultValue;
        } else{
            Logger.log("Unrecognized type " + attType + " for Alter Add!");
            return false;
        }

        Attribute newAttr = new Attribute(attName, def, defaultValue);
        List<Attribute> defs = tableSchema.getAttributes();
        defs.add(newAttr);




        try {
            //make new schema
            TableSchema newTable = new TableSchema(TEMP_TABLE_NAME, defs);

            StorageManager sm = StorageManager.getStorageManager();
            sm.CreateTable(newTable);

            //find start of old table
            Page pg = sm.selectFirstPage(tableName);

            //go through all pages
            while (pg != null) {
                int numRecords = pg.getNumRows();
                List<List<Object>> newRecords = new ArrayList<>();
                for (int i = 0; i < numRecords; i++) {
                    //get the old arrayList
                    ArrayList<Object> rec = new ArrayList<>(List.copyOf(pg.getRecord(i)));
                    //append the new record w/ default value to it
                    rec.add(defCast);

                    newRecords.add(rec);
                }
                //insert these records to new table
                sm.insert(TEMP_TABLE_NAME, newRecords);

                //move on to the next page of records from old table
                int nextPage = pg.getNextPage();
                if(nextPage!= -1) {
                    pg = sm.select(nextPage, tableName);
                }
                else{
                    pg = null;
                }

            }


            //now have copied all records through
            //delete old table and rename new one
            sm.DropTable(tableSchema);
            cat.renameTable(TEMP_TABLE_NAME, tableName);

        } catch (Exception e) {
            Logger.log(e.getMessage());
            throw new RuntimeException(e);
        }



        return true;
    }
}
