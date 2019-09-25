package group.bison.streaming.core.schema;

import group.bison.streaming.core.driver.DriverContext;

/**
 * Created by BSONG on 2019/9/15.
 */
public interface SchemaReader<T> {

    public T generateSchema(DriverContext driverContext, Long pid);
}
