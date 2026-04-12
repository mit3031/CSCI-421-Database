package AttributeInfo;

public abstract class AttributeDefinition {
    private final AttributeTypeEnum type;

    // null for most types other than CHAR and VARCHAR
    private final Integer maxLength;

    private final boolean isPrimary;
    private final boolean possibleNull;

    protected boolean isUnique;

    public AttributeDefinition(final AttributeTypeEnum type, final boolean isPrimary, final boolean possibleNull, boolean isUnique){
        this.type = type;
        this.isPrimary = isPrimary;
        this.possibleNull = possibleNull;
        this.maxLength = null;
        this.isUnique = isUnique;
    }

    public AttributeDefinition(final AttributeTypeEnum type, final boolean isPrimary, final boolean possibleNull, final Integer maxLength, final boolean isUnique){
        this.type = type;
        this.isPrimary = isPrimary;
        this.possibleNull = possibleNull;
        this.maxLength = maxLength;
        this.isUnique = isUnique;
    }

    public AttributeTypeEnum getType(){
        return this.type;
    }

    public boolean getIsPrimary(){
        return this.isPrimary;
    }

    public boolean getIsPossibleNull(){
        return this.possibleNull;
    }

    public Integer getMaxLength(){
        return this.maxLength;
    }

    public boolean getIsUnique() { return isUnique; }

    // returns whether or not a string is of this type
    public abstract boolean isType(String obj);

    // similar to above but checks type and if it follows the "rules"
    // for instance "hello world" is might be a VARCHAR but is not a VARCHAR(3)
    // likewise null is valid for anything that possibleNull
    // moral of the story this should probably be the main method of checking types
    public abstract boolean isValid(String obj);

    /*
       This function returns the size of whatever type of attribute is being defined
     */
    public int getByteSize() {
        switch (type) {
            case INTEGER: return Integer.BYTES;
            case DOUBLE:  return Double.BYTES;
            case BOOLEAN: return 1;
            case CHAR:    return maxLength;
            case VARCHAR: return maxLength; // upper bound for record sizing of varchar
            default: throw new RuntimeException("Unknown type");
        }
    }

}
