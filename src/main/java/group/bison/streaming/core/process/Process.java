package group.bison.streaming.core.process;

import java.io.Serializable;
import java.util.Properties;

/**
 * Created by BSONG on 2019/9/15.
 */
public class Process implements Serializable {

    private Long id;

    private String name;

    private Long parentId;

    private ProcessConfig config;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getParentId() {
        return parentId;
    }

    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }

    public ProcessConfig getConfig() {
        return config;
    }

    public void setConfig(ProcessConfig config) {
        this.config = config;
    }

    public static class ProcessConfig extends Properties {
        private Long id;

        private String ext;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getExt() {
            return ext;
        }

        public void setExt(String ext) {
            this.ext = ext;
        }
    }
}
