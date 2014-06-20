package systemlog;

import java.util.LinkedList;

/**
 * Created by George on 17/06/2014.
 */
public class SystemLog {
    static public  LinkedList<LogCell> log=new LinkedList<LogCell>();
    static boolean isAccepting=true;

    public static void clear() {
        log=new LinkedList<LogCell>();
    }

  public static String getAsString() {
        StringBuffer blob=new StringBuffer(1000);
        for(LogCell cell : log)
            blob.append(cell.toString() + "\n");
         blob.trimToSize();
        return blob.toString();
    }



    public static synchronized void add(OperationLogCell operationLogCell) {
        if(isAccepting)
            log.add(operationLogCell);
    }

    public static synchronized void add(SystemLogCell systemLogCell) {
        if(isAccepting)
            log.add(systemLogCell);
    }

    public static void stopAccepting() {
        isAccepting = false;
    }

    public static void startAccepting() {
        isAccepting = true;
    }
}
