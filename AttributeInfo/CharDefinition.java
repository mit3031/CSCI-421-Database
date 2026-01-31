package AttributeInfo;

import java.util.Objects;

public class CharDefinition extends AttributeDefinition {

    public CharDefinition(boolean isPrimary, boolean possibleNull, int maxLength) {
        super(AttributeTypeEnum.CHAR, isPrimary, possibleNull, maxLength);
    }

    @Override
    public boolean isType(String obj) {
        return obj.charAt(0) == '"' && obj.charAt(obj.length() - 1) == '"';
    }

    @Override
    public boolean isValid(String obj) {
        // Handle null case
        if (this.getIsPossibleNull() && obj.equals("null")) {
            return true;
        }

        if (!this.isType(obj)){
            return false;
        }

        // CHAR(N) must be exactly N characters
        return obj.length() == this.getMaxLength();
    }
}