import com.aerospike.client.policy.AuthMode;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Main {

    static String app = "reader";

    public static void main(String [] args)
    {
        try {
            loadProperties();
            if (app.equalsIgnoreCase("reader")) {
                new UDFExampleReporting().main();
            } else if (app.equalsIgnoreCase("writer"))
                new UDFExampleDataLoader().main();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static void loadProperties() throws IOException {
        Properties defaultProps = new Properties();
        String profile = "default.properties";
        String propertyFile = profile;
        FileInputStream in = new FileInputStream(propertyFile);
        defaultProps.load(in);
        in.close();

        app = defaultProps.getProperty("app");
    }
}
