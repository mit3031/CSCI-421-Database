package Common;

public class Logger {
    private static boolean debugOn;

    public static boolean initDebug(String[] args){
        for (String str: args){
            if (str.equals("--debug")) {
                Logger.debugOn = true;
                break;
            }
        }

        return Logger.debugOn;
    }

    public static void log(String str){
        if (Logger.debugOn){
            System.out.println(str);
        }

    }

    public static void logBase(String str){
        if (Logger.debugOn){
            System.out.print(str);
        }

    }

    public static void main(String[] args){
        Logger.initDebug(args);
        Logger.log("Hello World");

    }
}
