package DDLParser;

import AttributeInfo.*;
import Catalog.TableSchema;
import Common.Command;
import Common.Logger;
import StorageManager.StorageManager;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;

/**
 * Class responsible for creating table
 */
public class CreateTable implements Command {

    /**
     * attempts to run the create table command
     * @param command, the command split by spaces
     * @return True if good execution, false otherwise
     * @throws SQLSyntaxErrorException when command's syntax is bad
     */

    @Override
    public boolean run(String[] command) throws SQLSyntaxErrorException {
        int pKeyCount = 0; //for validation of exactly 1 key

        String tableName = command[2].toLowerCase(); //table name will always be 3rd "word" in command
        ArrayList<Attribute> attributes = new ArrayList<>();

        // Find the content between parentheses
        String fullCommand = String.join(" ", command);
        int openParen = fullCommand.indexOf("(");
        int closeParen = fullCommand.lastIndexOf(")"); // Use lastIndexOf to get the actual closing paren
        
        if (openParen == -1 || closeParen == -1) {
            throw new SQLSyntaxErrorException("Missing parentheses in CREATE TABLE statement");
        }
        
        String attributeSection = fullCommand.substring(openParen + 1, closeParen).trim();
        
        // Split by comma to get individual attribute definitions
        // Need to be careful not to split on commas inside CHAR(5) or VARCHAR(10)
        String[] attrDefs = splitByCommaRespectingParens(attributeSection);
        
        for (String attrDef : attrDefs) {
            try {
                String attrName = null;
                AttributeTypeEnum type = null;
                boolean primary = false;
                boolean notNull = false;
                int maxlength = 0;
                
                // Split this attribute definition by whitespace
                String[] tokens = attrDef.trim().split("\\s+");
                
                for (String token : tokens) {
                    if(attrName == null){
                        //convert to lowercase to allow respect to case insensitivity rules on attributes
                        attrName = token.toLowerCase();
                    } else if (token.equals("PRIMARYKEY")) {
                        primary=true;
                        pKeyCount++;
                    } else if (token.equals("NOTNULL")) {
                        notNull = true;
                    } else if (token.equals("INTEGER")) {
                        type = AttributeTypeEnum.INTEGER;
                    } else if (token.equals("DOUBLE")) {
                        type = AttributeTypeEnum.DOUBLE;
                    } else if (token.equals("BOOLEAN")){
                        type = AttributeTypeEnum.BOOLEAN;
                    } else if (token.indexOf("CHAR") == 0) {
                        type  = AttributeTypeEnum.CHAR;
                        String size = token.substring(
                                token.indexOf("(") + 1,
                                token.indexOf(")"));
                        maxlength = Integer.parseInt(size);
                    } else if (token.indexOf("VARCHAR") == 0){
                        type = AttributeTypeEnum.VARCHAR;
                        String size = token.substring(
                                token.indexOf("(") + 1,
                                token.indexOf(")"));
                        maxlength = Integer.parseInt(size);
                    } else{
                        Logger.log("Unrecognized term in table create: " + token);
                    }
                }
                
                //TODO Make attribute and then add
                AttributeDefinition definition;
                if (type == AttributeTypeEnum.INTEGER){
                    definition = new IntegerDefinition(type, primary, !notNull);
                } else if (type == AttributeTypeEnum.DOUBLE) {
                    definition = new DoubleDefinition(primary, !notNull);
                } else if (type == AttributeTypeEnum.BOOLEAN){
                    definition = new BooleanDefinition(primary, !notNull);
                } else if (type == AttributeTypeEnum.CHAR){
                    definition = new CharDefinition(primary, !notNull, maxlength);
                } else if (type == AttributeTypeEnum.VARCHAR) {
                    definition = new VarCharDefinition(primary, !notNull, maxlength);
                } else{
                    Logger.log("Create Table found an attribute with no definition!");
                    definition = null;
                }
                if(attrName == null){
                    throw new SQLSyntaxErrorException("Attribute does not have name!");
                }

                attributes.add(new Attribute(attrName, definition, null));

            }
            catch (IndexOutOfBoundsException e){
                Logger.log("CREATE Syntax Error: Out of bounds parsing attribute definitions");
                throw new SQLSyntaxErrorException("attribute definitions could not be parsed!");
            }
            catch (Exception e){
                Logger.log("CREATE Error: " + e.getMessage());
                throw new SQLSyntaxErrorException("attribute definitions could not be parsed!");
            }
        }

        
        
        
        //TODO check no attributes with same name
        //what do for not null? figure that out, not for this yet?

        if(pKeyCount > 1){
            Logger.log("CREATE Syntax Error: Primary Key Count is: " + pKeyCount + ". Expected 0 or 1");
            return false;
        }
        TableSchema table = new TableSchema(tableName, attributes);

        //TODO do the storage manager call and see result, return based on that
        StorageManager sm = StorageManager.getStorageManager();
        try {
            sm.CreateTable(table);
        } catch (Exception e) {
            Logger.log("Create Table Error in S.M. call: " + e.getMessage());
            throw new RuntimeException(e);
        }
        Logger.log("Command parsed successfully");
        return true;
    }
    
    /**
     * Splits a string by commas, but not commas inside parentheses
     * For example: "d1 DOUBLE, c1 CHAR(5,3), v1 VARCHAR(10)" 
     * returns ["d1 DOUBLE", "c1 CHAR(5,3)", "v1 VARCHAR(10)"]
     */
    private String[] splitByCommaRespectingParens(String input) {
        ArrayList<String> result = new ArrayList<>();
        int parenDepth = 0;
        int start = 0;
        
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            
            if (c == '(') {
                parenDepth++;
            } else if (c == ')') {
                parenDepth--;
            } else if (c == ',' && parenDepth == 0) {
                // Found a comma outside parentheses - this is a separator
                result.add(input.substring(start, i).trim());
                start = i + 1;
            }
        }
        
        // Add the last segment
        if (start < input.length()) {
            result.add(input.substring(start).trim());
        }
        
        return result.toArray(new String[0]);
    }
}
