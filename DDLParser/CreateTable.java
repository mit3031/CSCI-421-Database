package DDLParser;

import AttributeInfo.*;
import Common.Command;
import Common.Logger;

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

        String tableName = command[2]; //table name will always be 3rd "word" in command
        ArrayList<Attribute> attributes = new ArrayList<>();

        int index = 4;
        //keep parsing until out of stuff
        while(index<command.length && command[index].contains(")")){
            try {
                String attrName = null;
                AttributeTypeEnum type = null;
                boolean primary = false;
                boolean notNull = false;
                int maxlength = 0; //only for varchars

                while (!command[index].contains(",") &&
                !command[index].contains(")")){
                    
                    if(attrName == null){
                        //convert to lowercase to allow respect to case insensitivity rules on attributes
                        attrName = command[index].toLowerCase();
                    } else if (command[index].equals("PRIMARYKEY")) {
                        primary=true;
                        pKeyCount++;
                    } else if (command[index].equals("NOTNULL")) {
                        notNull = true;
                    } else if (command[index].equals("INTEGER")) {
                        type = AttributeTypeEnum.INTEGER;
                    } else if (command[index].equals("DOUBLE")) {
                        type = AttributeTypeEnum.DOUBLE;
                    } else if (command[index].equals("BOOLEAN")){
                        type = AttributeTypeEnum.BOOLEAN;
                    } else if (command[index].indexOf("CHAR") == 0) {
                        type  = AttributeTypeEnum.CHAR;
                        String size = command[index].substring(
                                command[index].indexOf("(") + 1,
                                command[index].indexOf(")"));
                        maxlength = Integer.parseInt(size);
                    } else if (command[index].indexOf("VARCHAR") == 0){
                        type = AttributeTypeEnum.VARCHAR;
                        String size = command[index].substring(
                                command[index].indexOf("(") + 1,
                                command[index].indexOf(")"));
                        maxlength = Integer.parseInt(size);
                    } else{
                        Logger.log("Unrecognized term in table create: " + command[index]);
                    }
                    //todo figure out typing, get maxLength if applicable (varchar situation)



                    index++;
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

                attributes.add(new Attribute(attrName, definition, null));

                //TODO add attribute to list
            }
            catch (IndexOutOfBoundsException e){
                Logger.log("CREATE Syntax Error: Out of bounds parsing attribute definitions");
                throw new SQLSyntaxErrorException("attribute definitions could not be parsed!");
                //obviously did not succeed because of the error
            }
        }

        
        
        
        //TODO check no attributes with same name
        //also check exactly one thign for primary key
        //what do for not null? figure that out

        if(pKeyCount != 1){
            Logger.log("CREATE Syntax Error: Primary Key Count is: " + pKeyCount + ". Expected 1");
            return false;
        }

        //TODO do the storage manager call and see result, return based on that





        return false;
    }
}
