package group.bison.streaming.spark.driver;

import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.core.driver.StreamingDriver;

import java.util.Properties;

/**
 * Created by BSONG on 2019/10/6.
 */
public class SparkDriver implements StreamingDriver {

    @Override
    public DriverContext getContext(Properties properties) throws IllegalArgumentException {
        return null;
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }
}
