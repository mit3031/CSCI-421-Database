package DDLParser;

import AttributeInfo.Attribute;
import Catalog.Catalog;
import Catalog.TableSchema;
import Common.Command;
import Common.Logger;

import java.sql.SQLSyntaxErrorException;

/**
 * Class to handle running an alter table, in which an attribute is added
 */
public class AlterTableAdd implements Command {

    private static final int MIN_QUERY_LEN = 6;
    private static final int TABLE_NAME_INDEX = 2;
    private static final int NEW_ATT_NAME_INDEX = 4;
    private static final int NEW_ATT_TYPE_INDEX = 5;

    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException {
        if(command.length <MIN_QUERY_LEN) {
            Logger.log("Command too short for Alter Table Drop! Min. Len " +  MIN_QUERY_LEN + " Got " + command.length);
        }

        String tableName =  command[TABLE_NAME_INDEX];
        Catalog cat = Catalog.getInstance();
        TableSchema tableSchema = cat.getTable(tableName);
        //we do not have table
        if(tableSchema == null) {
            Logger.log("Table " + tableName + " not found from Catalog!");
            return false;
        }

        String attName = command[NEW_ATT_NAME_INDEX];
        String attType = command[NEW_ATT_TYPE_INDEX];


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

        boolean notNull = false;
        //now we can get rest of info about this
        for(int i = 6; i<command.length; i++) {
            if(command[i].equals("NOTNULL")) {
                notNull = true;
            }
            else if(command[i].equals("DEFAULT")) {
                i++;
                if(i<command.length) {
                    //TODO figure out default value in context of typings
                }
            }
            else{
                Logger.log("Default specified but not given value");
            }
        }











        return false;
    }
}
