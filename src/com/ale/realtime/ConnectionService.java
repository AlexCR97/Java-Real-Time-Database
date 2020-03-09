package com.ale.realtime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class ConnectionService {
    
    private final String host;
    private final String port;
    private final String database;
    private final String user;
    private final String password;
    private Connection connection;

    public ConnectionService(String host, String port, String database, String user, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.password = password;
    }
    
    public CompletableFuture<Connection> getConnection() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String url = getConnectionUrl();
                connection = DriverManager.getConnection(url, user, password);
                
                return connection;
            }
            catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    public String getConnectionUrl() {
        return String.format(
                "jdbc:mysql://%s:%s/%s?"
                        + "useUnicode=true&"
                        + "useJDBCCompliantTimezoneShift=true&"
                        + "useLegacyDatetimeCode=false&"
                        + "serverTimezone=UTC",
                host,
                port,
                database
        );
    }
    
}
