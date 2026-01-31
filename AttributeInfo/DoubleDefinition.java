package AttributeInfo;

public class DoubleDefinition extends AttributeDefinition {

    public DoubleDefinition(boolean isPrimary, boolean possibleNull) {
        super(AttributeTypeEnum.DOUBLE, isPrimary, possibleNull);
    }

    private static boolean isDouble(String s) {
        try {
            double d = Double.parseDouble(s);
            // Per your DMLParser comments: DOUBLE is non-negative
            return d >= 0;
        } catch (NumberFormatException | NullPointerException e) {
            return false;
        }
    }

    @Override
    public boolean isType(String obj) {
        return isDouble(obj);
    }

    @Override
    public boolean isValid(String obj) {
        if (this.getIsPossibleNull() && obj.equals("null")) {
            return true;
        }

        return this.isType(obj);
    }
}