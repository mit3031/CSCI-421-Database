package AttributeInfo;

import java.util.Objects;

public class IntegerDefinition extends AttributeDefinition{

    public IntegerDefinition(AttributeTypeEnum type, boolean isPrimary, boolean possibleNull) {
        super(AttributeTypeEnum.INTEGER, isPrimary, possibleNull);
    }

    private static boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
        } catch(NumberFormatException | NullPointerException e) {
            return false;
        }
        // only got here if we didn't return false
        return true;
    }

    @Override
    public boolean isType(String obj) {
        return isInteger(obj);
    }

    @Override
    public boolean isValid(String obj) {
        if (this.getIsPossibleNull() && obj.equalsIgnoreCase("null")){
            return true;
        }

        return this.isType(obj);
    }
}
