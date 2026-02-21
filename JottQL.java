import Common.Logger;
import DDLParser.ParserDDL;
import DMLParser.ParserDML;
import StorageManager.StorageManager;

import java.io.IOException;
import java.util.Scanner;

/**
 * Main class for running the entire Project
 */
public class JottQL {
    public static final String QUIT_MESSAGE = "<QUIT>";

    /**
     * Initialize Database here
     */
    private static void startUp(String dbLocation, int pageSize, int bufferSize,
                                        boolean indexing, boolean debug){
        try {
            StorageManager.initDatabase(dbLocation, pageSize, bufferSize);
            StorageManager storageManager = StorageManager.getStorageManager();
            storageManager.bootup();
            if(debug){
                String[] pseudoArgs = new String[1];
                pseudoArgs[0] = "--debug";
                Logger.initDebug(pseudoArgs);
            }
        } catch (Exception e) {
            System.out.println("An error occurred, could not start Database!");
            System.exit(1);
        }


    }

    /**
     * Handle shutdown procedure here
     */
    private static void shutdown() throws IOException {
        System.out.println("Shutting down the database...");
        StorageManager storageManager = StorageManager.getStorageManager();
        storageManager.shutdown();
        System.out.println("Database shutdown complete");
    }

    /**
     * The main program and loop
     * @param args Command-line arguments in the following order:
     *             dbLocation: directory of database
     *             pageSize: byte size of page
     *             bufferSize: number of pages buffer can hold
     *             indexing: True/False: for indexing
     *             debug: True/False: enables Logging
     */
    public static void main(String[] args) {

        if(args.length >5 || args.length < 4){
            System.out.println("Usage: java jottQL <dblocation> <pageSize> <bufferSize> <indexing>");
            return;
        }
        System.out.println("Welcome to JottQL!");


            String dbLocation = args[0];
            int pageSize = Integer.parseInt(args[1]);
            int bufferSize = Integer.parseInt(args[2]);
            boolean indexing = Boolean.parseBoolean(args[3]);
            boolean debug = false;
            if (args.length == 5) {
                debug = Boolean.parseBoolean(args[4]);
            }

            startUp(dbLocation, pageSize, bufferSize, indexing, debug);
            Scanner input = new Scanner(System.in);

            while (true) {
                try {
                System.out.print("JottQL> ");
                boolean commandReady = false;
                String message = "";
                while(!commandReady) {


                    message += input.nextLine();
                    if(message.equals(QUIT_MESSAGE)) {
                        shutdown();
                        return;
                    }
                    if(message.contains(";")){
                        commandReady = true;
                        //trim message to end in semicolon
                        message = message.substring(0, message.indexOf(";") + 1);
                    }
                }

                String[] keywords = message.split("\\s+");

                if (message.equals(QUIT_MESSAGE)) {
                    shutdown();
                    return;

                } else if (keywords[0].equals("CREATE") ||
                        keywords[0].equals("ALTER") ||
                        keywords[0].equals("DROP")){
                    //DDL parser handles
                    Logger.log("Command Sent to DDL Parser");
                    ParserDDL.parseCommand(message);

                } else if (keywords[0].equals("SELECT") || keywords[0].equals("INSERT")) {
                    //DML parser handles
                    Logger.log(("Command Sent to DML Parser"));
                    try {
                        ParserDML.runCommand(message);
                    } catch (java.sql.SQLSyntaxErrorException e) {
                        System.out.println("Syntax Error: " + e.getMessage());
                    } catch (Exception e) {
                        System.out.println("Error executing DML command: " + e.getMessage());
                        if(debug) e.printStackTrace();
                    }
                }
                else{ //does not match any of our cases
                    System.out.println("Unrecognized command in following input:\n" + message);
                }

                }
                catch(Exception e){
                    //TODO do something here or move inside while true loop so we can loop gracefully when encounter error
                }

            }
    }
}
