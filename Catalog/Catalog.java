package Catalog;

import AttributeInfo.*;
import java.io.*;
import java.util.*;

public class Catalog {

    /*/
    This keeps the catalog as a singleton (aka only one catalog can exist)
     */
    private static Catalog instance;

    public static void init(String dbPath, int pageSize) {
        if (instance == null) {
            instance = new Catalog(dbPath, pageSize);
        }
    }

    public static Catalog getInstance() {
        if (instance == null) {
            throw new IllegalStateException("Catalog not initialized");
        }
        return instance;
    }

    // Only for testing purposes: resets singleton to simulate program restart
    public static void resetForTesting() {
        instance = null;
    }

    /*
    Catalog setup for the actual state of a catalog
     */
    private final Map<String, TableSchema> tables;
    private final String catalogPath;

    // NOTE: firstFreePage stored here for simplicity
    // May move to StorageManager metadata in later phases.
    private int pageSize;        // Required for DB restart
    private int firstFreePage;   // Location of first empty page (-1 if none)

    private Catalog(String dbPath, int pageSize) {
        this.tables = new HashMap<>();
        this.catalogPath = dbPath + "/catalog.bin";

        // This ensures the directory exists before we try to save/load anything.
        new File(dbPath).mkdirs();

        this.pageSize = pageSize;      // Default if no file exists
        this.firstFreePage = 0;       // by default first free page is at index 0
        loadFromDisk();
    }

    /*
    Getters and setters for page info
     */
    public int getPageSize(){
        return this.pageSize;
    }

    public int getFirstFreePage() {
        return this.firstFreePage;
    }

    public void setFirstFreePage(int pageId) {
        this.firstFreePage = pageId;
        saveToDisk();
    }

    /*
    Public methods below
    /
     */
    public void addTable(TableSchema table) {
        String name = table.getTableName().toLowerCase();
        if (tables.containsKey(name)) {
            throw new RuntimeException("Table already exists: " + name);
        }
        tables.put(name, table);
        saveToDisk();
    }

    public void dropTable(String tableName) {
        tables.remove(tableName.toLowerCase());
        saveToDisk();
    }

    public TableSchema getTable(String tableName) {
        return tables.get(tableName.toLowerCase());
    }


    /**
     * Note: Attribute acts as an extension to attribute defintion, this does not get the actual instance of the
     * attributes, just lets you gather info on them
     * @param tableName: name of the table attributes you want to get info on
     * @return list of the attributes for this table
     */
    public List<Attribute> getAttribute(String tableName){
        return tables.get(tableName.toLowerCase()).getAttributes();
    }

    /**
     * Gets the address of the first page instance
     * @param tableName: name of the table
     * @return the address of the first table
     */
    public int getAddressOfPage(String tableName) throws Exception {
        TableSchema table = tables.get(tableName.toLowerCase());

        if (table == null){
            throw new Exception("Table does not exists yet");
        }

        if (table.getRootPageID() == -1){
            // abort, something is wrong
            throw new Exception("Table does not exist yet");
        }

        return table.getRootPageID() * pageSize;
    }

    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName.toLowerCase());
    }

    public Collection<TableSchema> getAllTables() {
        return tables.values();
    }

    public void saveToDisk() {
        DataOutputStream out = null;

        try {
            // binary output stream to the catalog file
            FileOutputStream fos = new FileOutputStream(catalogPath);
            out = new DataOutputStream(fos);

            // Write global database info first
            out.writeInt(this.pageSize);
            out.writeInt(this.firstFreePage);

            // Writes how many tables exist in the catalog
            out.writeInt(tables.size());

            // Loops through every table in the catalog
            for (TableSchema table : tables.values()) {

                // Writes the table name
                out.writeUTF(table.getTableName());

                // Writes Page ID (location)
                out.writeInt(table.getRootPageID());

                // Get table attributes
                List<Attribute> attrs = table.getAttributes();

                // Write how many attributes this table has
                out.writeInt(attrs.size());

                // Loops through each attribute
                for (Attribute attr : attrs) {

                    // Write the attribute name
                    out.writeUTF(attr.getName());

                    // Get the attribute's definition (type + constraints)
                    AttributeDefinition def = attr.getDefinition();

                    // Write the attribute type as an integer code (helper function below)
                    out.writeInt(getEnumCode(def.getType()));

                    // Write constraints
                    out.writeBoolean(def.getIsPrimary());
                    out.writeBoolean(def.getIsPossibleNull());

                    // Write max length
                    if (def.getMaxLength() == null) {
                        out.writeInt(0); // not applicable
                    } else {
                        out.writeInt(def.getMaxLength());
                    }

                    // Write the default value using a boolean flag
                    if (attr.getDefaultValue() != null) {
                        out.writeBoolean(true);           // indicates a default value exists
                        out.writeUTF(attr.getDefaultValue()); // write the actual string
                    } else {
                        out.writeBoolean(false);          // no default value
                    }
                }
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to write catalog to disk", e);

        } finally {
            // Closes stream always
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                // Ignores close error
            }
        }
    }

    private void loadFromDisk() {

        // Check if catalog exists
        File file = new File(catalogPath);
        if (!file.exists()) {
            // No catalog yet so fresh database
            saveToDisk(); // since this is a new database we're good to store its metadata
            return;
        }

        DataInputStream in = null;

        try {
            // Opens a binary input stream to read the catalog
            FileInputStream fis = new FileInputStream(catalogPath);
            in = new DataInputStream(fis);

            // Read global database info first
            this.pageSize = in.readInt();
            this.firstFreePage = in.readInt();

            // Read how many tables are stored
            int numTables = in.readInt();

            // Loop through each table
            for (int i = 0; i < numTables; i++) {

                // Read table name
                String tableName = in.readUTF();

                // Read Page ID (location)
                int rootPageID = in.readInt();

                // Read how many attributes this table has
                int numAttrs = in.readInt();

                // Temporary list to store attributes
                List<Attribute> attrs = new ArrayList<>();

                // Loop through each attribute
                for (int j = 0; j < numAttrs; j++) {

                    // Read attribute name
                    String attrName = in.readUTF();

                    // Read and convert the type code back to an enum (helper function below)
                    int typeCode = in.readInt();
                    AttributeTypeEnum type = getEnumFromCode(typeCode);

                    // Read constraints
                    boolean isPrimary = in.readBoolean();
                    boolean isNullable = in.readBoolean();

                    // Read max length
                    int rawMaxLen = in.readInt();
                    Integer maxLen;
                    if (rawMaxLen == 0) {
                        maxLen = null;
                    } else {
                        maxLen = rawMaxLen;
                    }

                    // Read default value using boolean flag
                    boolean hasDefault = in.readBoolean();
                    String defaultValue;
                    if (hasDefault) {
                        defaultValue = in.readUTF(); // read the actual value
                    } else {
                        defaultValue = null;         // no default
                    }

                    // Create the AttributeDefinition
                    AttributeDefinition def;

                    if (type == AttributeTypeEnum.INTEGER) {
                        def = new IntegerDefinition(AttributeTypeEnum.INTEGER, isPrimary, isNullable);

                    } else if (type == AttributeTypeEnum.DOUBLE) {
                        def = new DoubleDefinition(isPrimary, isNullable);

                    } else if (type == AttributeTypeEnum.BOOLEAN) {
                        def = new BooleanDefinition(isPrimary, isNullable);

                    } else if (type == AttributeTypeEnum.CHAR) {
                        // chars use the maxLen we read from the disk
                        def = new CharDefinition(isPrimary, isNullable, maxLen);

                    } else if (type == AttributeTypeEnum.VARCHAR) {
                        // varchars also uses the maxLen
                        def = new VarCharDefinition(isPrimary, isNullable, maxLen);

                    } else {
                        // This should only happen if the file is corrupted or a new type was added
                        throw new RuntimeException("Unknown attribute type code found in catalog file.");
                    }

                    // Creates Attribute object and add it to the list
                    Attribute attr = new Attribute(attrName, def, defaultValue);
                    attrs.add(attr);
                }

                // Rebuilds the table schema and add it to the catalog
                TableSchema schema = new TableSchema(tableName, attrs, rootPageID);
                tables.put(tableName.toLowerCase(), schema);
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to load catalog from disk", e);

        } finally {
            // Closes the stream always
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                //Ignoring close errors
            }
        }
    }

    // Apparently storing ENUMS directly in our binary storage isn't the best idea
    // It takes extra space so we tie it to in integer value
    // Also better for binary persistence

    /*
    Helper function for saving stuff to the binary database (just converts enums -> integers)
     */
    private int getEnumCode(AttributeTypeEnum type) {
        switch (type) {
            case INTEGER: return 1;
            case DOUBLE:  return 2;
            case BOOLEAN: return 3;
            case CHAR:    return 4;
            case VARCHAR: return 5;
            default: throw new RuntimeException("Unknown type");
        }
    }

    /*
    Helper function for loading stuff from the binary database (just converts integers -> back to ENUMS)
     */
    private AttributeTypeEnum getEnumFromCode(int code) {
        switch (code) {
            case 1: return AttributeTypeEnum.INTEGER;
            case 2: return AttributeTypeEnum.DOUBLE;
            case 3: return AttributeTypeEnum.BOOLEAN;
            case 4: return AttributeTypeEnum.CHAR;
            case 5: return AttributeTypeEnum.VARCHAR;
            default: throw new RuntimeException("Invalid type code");
        }
    }
}