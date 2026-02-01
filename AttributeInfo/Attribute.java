package AttributeInfo;

public class Attribute {
    private final String name;
    private final AttributeDefinition definition;
    private final String defaultValue;

    //Use this when there IS a default value (ALTER TABLE ADD)
    public Attribute(String name, AttributeDefinition definition, String defaultValue) {
        this.name = name.toLowerCase(); // Enforce case-insensitivity here
        this.definition = definition;
        this.defaultValue = defaultValue;
    }

    // Use this when there is NO default value (CREATE TABLE)
    public Attribute(String name, AttributeDefinition definition) {
        this(name, definition, null);
    }

    public String getName() {
        return name;
    }

    public AttributeDefinition getDefinition() {
        return definition;
    }

    public String getDefaultValue() {
        return defaultValue;
    }
}