package group.bison.streaming.core.driver;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by BSONG on 2019/9/15.
 */
public class DriverManager {

    private static final Map<String, StreamingDriver> driverMap = new ConcurrentHashMap<>();

    public static DriverContext create(String driver, Properties properties) throws IllegalArgumentException {
        if (!driverMap.containsKey(driver)) {
            try {
                Class<StreamingDriver> streamingDriverClass = (Class<StreamingDriver>) Class.forName(driver);
                StreamingDriver streamingDriver = streamingDriverClass.newInstance();
                driverMap.put(driver, streamingDriver);
            } catch (Exception e) {
                throw new IllegalArgumentException("driver:" + driver + " not exists, or is not StreamingDriver");
            }
        }

        DriverContext driverContext = driverMap.get(driver).getContext(properties);
        return driverContext;
    }

    public static void register(StreamingDriver streamingDriver) {
        if (driverMap.containsKey(streamingDriver.getClass().getName())) {
            return;
        }

        driverMap.put(streamingDriver.getClass().getName(), streamingDriver);
    }
}
