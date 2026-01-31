package AttributeInfo;

public abstract class AttributeDefinition {
    private final AttributeTypeEnum type;

    // null for most types other than CHAR and VARCHAR
    private final Integer maxLength;

    private final boolean isPrimary;
    private final boolean possibleNull;

    public AttributeDefinition(final AttributeTypeEnum type, final boolean isPrimary, final boolean possibleNull){
        this.type = type;
        this.isPrimary = isPrimary;
        this.possibleNull = possibleNull;
        this.maxLength = null;
    }

    public AttributeDefinition(final AttributeTypeEnum type, final boolean isPrimary, final boolean possibleNull, final Integer maxLength){
        this.type = type;
        this.isPrimary = isPrimary;
        this.possibleNull = possibleNull;
        this.maxLength = maxLength;
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

    // returns whether or not a string is of this type
    public abstract boolean isType(String obj);

    // similar to above but checks type and if it follows the "rules"
    // for instance "hello world" is might be a VARCHAR but is not a VARCHAR(3)
    // likewise null is valid for anything that possibleNull
    // moral of the story this should probably be the main method of checking types
    public abstract boolean isValid(String obj);



}
