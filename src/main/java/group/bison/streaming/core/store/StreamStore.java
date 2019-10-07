package group.bison.streaming.core.store;

import group.bison.streaming.core.driver.DriverContext;

/**
 * Created by BSONG on 2019/9/15.
 */
public interface StreamStore<T, E> {

    E persist(DriverContext driverContext, Long pid, T schema);
}
