package group.bison.process.core.model;

import java.io.Serializable;
import java.lang.*;
import java.lang.Process;
import java.util.Properties;

/**
 * Created by BSONG on 2019/9/15.
 */
public class ProcessNode implements Serializable {

    private Long id;

    private String name;

    private Long processId;

    private java.lang.Process process;

    private Long parentId;

    private String nextId;

    private ProcessNodeConfig config;

    public ProcessNode getById(Long processNodeId) {
        return this;
    }

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

    public Long getProcessId() {
        return processId;
    }

    public void setProcessId(Long processId) {
        this.processId = processId;
    }

    public java.lang.Process getProcess() {
        return process;
    }

    public void setProcess(Process process) {
        this.process = process;
    }

    public Long getParentId() {
        return parentId;
    }

    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }

    public String getNextId() {
        return nextId;
    }

    public void setNextId(String nextId) {
        this.nextId = nextId;
    }

    public ProcessNodeConfig getConfig() {
        return config;
    }

    public void setConfig(ProcessNodeConfig config) {
        this.config = config;
    }

    public static class ProcessNodeConfig extends Properties {
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
