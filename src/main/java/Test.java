import java.io.FileInputStream;
import java.io.ObjectInputStream;
import static java.lang.System.*;

/**
 *
 */
public class Test {

    public static void main(String ... args) {

        Object params = readObject("/tmp/params.bin");

        out.println(params.getClass());

        return;
    }

    private static <T> T readObject(String file) {
        FileInputStream fis = null;
        ObjectInputStream ois = null;
        try {
            fis = new FileInputStream(file);
            ois = new ObjectInputStream(fis);
            return (T) ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        } finally{
            try{
                ois.close();
                fis.close();
            } catch ( Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }

}
