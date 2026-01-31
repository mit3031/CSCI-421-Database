package AttributeInfo;

import java.util.Objects;

public class VarCharDefinition extends AttributeDefinition {

    public VarCharDefinition(boolean isPrimary, boolean possibleNull, int maxLength) {
        super(AttributeTypeEnum.VARCHAR, isPrimary, possibleNull, maxLength);
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

        // VARCHAR(N) must be <= N characters
        return obj.length() <= this.getMaxLength();
    }
}