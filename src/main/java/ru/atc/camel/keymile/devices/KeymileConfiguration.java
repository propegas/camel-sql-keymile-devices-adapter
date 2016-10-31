package ru.atc.camel.keymile.devices;

import org.apache.camel.spi.UriParams;

@UriParams
public class KeymileConfiguration {

    private String username;

    private String password;

    private String source;

    private String adaptername;

    private String serviceNodeGroup;

    private String postgresqlDb;

    private String postgresqlHost;

    private String tablePrefix;

    private String postgresqlPort;

    private String query;

    private int delay;

    public String getTablePrefix() {
        return tablePrefix;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPostgresqlPort() {
        return postgresqlPort;
    }

    public void setPostgresqlPort(String postgresqlPort) {
        this.postgresqlPort = postgresqlPort;
    }

    public String getPostgresqlHost() {
        return postgresqlHost;
    }

    public void setPostgresqlHost(String postgresqlHost) {
        this.postgresqlHost = postgresqlHost;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getPostgresqlDb() {
        return postgresqlDb;
    }

    public void setPostgresqlDb(String postgresqlDb) {
        this.postgresqlDb = postgresqlDb;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getAdaptername() {
        return adaptername;
    }

    public void setAdaptername(String adaptername) {
        this.adaptername = adaptername;
    }

    public String getServiceNodeGroup() {
        return serviceNodeGroup;
    }

    public void setServiceNodeGroup(String serviceNodeGroup) {
        this.serviceNodeGroup = serviceNodeGroup;
    }

}