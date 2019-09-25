package group.bison.streaming.core.driver;

import java.util.Properties;

/**
 * Created by BSONG on 2019/9/15.
 */
public interface StreamingDriver {

    DriverContext getContext(Properties properties) throws IllegalArgumentException;

    int getMajorVersion();

    int getMinorVersion();
}
