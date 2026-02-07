package StorageManager;

import Catalog.Catalog;
import java.io.File;
import Common.Logger;

public class StorageManager {
    private static StorageManager storageManager;
    private String dbPath = "";
    private String databaseFilePath = "";

    private StorageManager(String dbPath, int pageSize, int bufferSize) throws Exception {
        this.dbPath = dbPath;

        Logger.log("db path is " + dbPath);

        if (dbPath.trim().isEmpty()){
            throw new Exception("Not a valid directory");
        }

        // 1. Check if the data directory exists
        File dbDir = new File(dbPath);
        if (!dbDir.exists()) {
            if (dbDir.mkdirs()) {
                Logger.log("Database directory created at: " + dbPath);
            } else {
                throw new Exception("Failed to create directory: " + dbPath);
            }
        }

        // 2. Create the database.bin file inside the directory
        // Use File.separator to ensure this works on both Windows and Linux/Mac
        File dbFile = new File(dbPath + File.separator + "database.bin");
        this.databaseFilePath = dbPath + File.separator + "database.bin";

        if (!dbFile.exists()) {
            try {
                if (dbFile.createNewFile()) {
                    Logger.log("Database file created: " + dbFile.getAbsolutePath());
                }
            } catch (Exception e) {
                throw new Exception("Could not create database.bin: " + e.getMessage());
            }
        } else {
            Logger.log("Database file already exists, skipping creation.");
        }

        // From here, init the catalog.
        Catalog.init(dbPath, pageSize);
    }

    // Updated to accept parameters needed for the constructor
    private static void createStorageManager(String dbPath, int pageSize, int bufferSize) throws Exception {
        storageManager = new StorageManager(dbPath, pageSize, bufferSize);
    }

    public static void initDatabase(String dbPath, int pageSize, int bufferSize) throws Exception {
        if (storageManager == null){
            createStorageManager(dbPath, pageSize, bufferSize);
        }
    }

    // Getter for the singleton instance
    public static StorageManager getStorageManager() {
        return storageManager;
    }

    public static void main(String[] args){
        String[] debugActive = new String[]{"--debug"};
        Logger.initDebug(debugActive);
        try {
            StorageManager.initDatabase(args[0], 400, 0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}