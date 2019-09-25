package group.bison.streaming.core.transforms;

import group.bison.streaming.core.driver.DriverContext;

/**
 * Created by BSONG on 2019/9/15.
 */
public interface Transformation<S1, S2> {

    S2 transform(DriverContext driverContext, Long pid, S1 inStream);
}
