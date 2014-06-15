package megastore;

/**
 * Created by George on 15/06/2014.
 */
public class LogBuffer {
    static private StringBuffer buffer=new StringBuffer(1000);

    public static synchronized void  println(String s) {
        buffer.append(s + "\n");
    }

    public static void clear() {
        buffer=new StringBuffer(1000);
    }

    public static String getAsString() {
        buffer.trimToSize();
        return buffer.toString();
    }
}
