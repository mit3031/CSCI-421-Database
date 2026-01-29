package Common;

/**
 * A lightweight, static utility for handling debug-level logging.
 * <p>
 * This class maintains a global state for debugging. It must be initialized
 * using {@code initDebug(String[])} typically at the start of the application.
 */
public class Logger {

    /** Flag indicating if debug mode is active. Defaults to false. */
    private static boolean debugOn;

    /**
     * Scans command-line arguments to enable debug mode.
     * * @param args The array of strings passed to the application's main method.
     * @return true if the {@code --debug} flag was found, false otherwise.
     */
    public static boolean initDebug(String[] args){
        for (String str: args){
            if (str.equals("--debug")) {
                Logger.debugOn = true;
                break;
            }
        }
        return Logger.debugOn;
    }

    /**
     * Prints a message to the standard output followed by a newline,
     * but only if debug mode is enabled.
     * * @param str The message to log.
     */
    public static void log(String str){
        if (Logger.debugOn){
            System.out.println(str);
        }
    }

    /**
     * Prints a message to the standard output without a newline,
     * but only if debug mode is enabled. Useful for building multi-part logs.
     * * @param str The message to log.
     */
    public static void logBase(String str){
        if (Logger.debugOn){
            System.out.print(str);
        }
    }

    /**
     * Entry point for testing the Logger functionality directly.
     * @param args Command line arguments.
     */
    public static void main(String[] args){
        Logger.initDebug(args);
        Logger.log("Hello World");
    }
}