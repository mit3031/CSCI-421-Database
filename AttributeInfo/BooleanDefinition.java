package AttributeInfo;

import java.util.Objects;

public class BooleanDefinition extends AttributeDefinition {

    public BooleanDefinition(boolean isPrimary, boolean possibleNull) {
        super(AttributeTypeEnum.BOOLEAN, isPrimary, possibleNull);
    }

    private static boolean isBoolean(String s) {
        if (s == null) return false;
        return s.equals("True") || s.equals("False");
    }

    @Override
    public boolean isType(String obj) {
        return isBoolean(obj);
    }

    @Override
    public boolean isValid(String obj) {
        if (this.getIsPossibleNull() && obj.equals("null")) {
            return true;
        }

        return this.isType(obj);
    }
}