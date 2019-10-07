package group.bison.streaming.core;

import group.bison.process.core.model.ProcessNode;
import group.bison.process.core.model.ProcessO;
import group.bison.process.core.model.ProcessTopo;
import group.bison.process.core.service.ProcessService;
import group.bison.process.core.service.ProcessTopoService;
import group.bison.streaming.core.driver.DriverContext;
import group.bison.streaming.core.driver.DriverManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Properties;
import java.util.function.Function;

/**
 * Created by BSONG on 2019/9/15.
 */
public class StreamingEngine {
    private static final Logger LOG = LogManager.getLogger(StreamingEngine.class);

    public static void start(Long processId, Function fallBack) {
        try {
            ProcessService processService = null;
            ProcessTopoService processTopoService = null;

            //1. 加载process信息
            ProcessO process = processService.getProcessById(processId);
            List<ProcessNode> processNodeList = processService.getProcessNodes(processId);
            ProcessTopo processTopo = processTopoService.getProcessTopo(processId);
            //todo 校验topo信息，执行是否有问题，有则直接拦截

            //2. 初始化driver
            Properties properties = new Properties();
            properties.setProperty("processId", processId.toString());
            properties.setProperty("processTopo", processTopo.toString());
            String driverName = process.getConfig().getProperty("driver");
            DriverContext driverContext = DriverManager.create(driverName, properties);

            //3. driver启动
            driverContext.start();

            //4. 监听执行结果,触发异常回调
            new Thread(() -> {
                while (true) {
                    try {
                        int phase = driverContext.getPhase();
                        if (phase == -1) {
                            fallBack.apply(new RuntimeException("处理失败", (Throwable) driverContext.getProperties().get("exception")));
                        }
                    } finally {
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            }).start();

        } catch (Exception e) {
            LOG.error("启动引擎失败", e);

            fallBack.apply(new RuntimeException("启动引擎失败", e));
        }
    }
}
