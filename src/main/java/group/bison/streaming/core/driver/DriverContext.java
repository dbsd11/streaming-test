package group.bison.streaming.core.driver;

import org.springframework.context.SmartLifecycle;

import java.util.Properties;

/**
 * Created by BSONG on 2019/9/15.
 */
public interface DriverContext extends SmartLifecycle {

    String getId();

    Properties getProperties();
}
