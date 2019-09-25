package group.bison.streaming.core.source;

import group.bison.streaming.core.driver.DriverContext;

/**
 * Created by BSONG on 2019/9/15.
 */
public interface StreamSource<T, S> {

    S streamOf(DriverContext driverContext, Long pid, T schema);
}
