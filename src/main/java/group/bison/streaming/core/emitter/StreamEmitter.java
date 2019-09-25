package group.bison.streaming.core.emitter;

import group.bison.streaming.core.driver.DriverContext;

/**
 * Created by BSONG on 2019/9/15.
 */
public interface StreamEmitter<T, E> {

    E persist(DriverContext driverContext, Long pid, T schema);
}
