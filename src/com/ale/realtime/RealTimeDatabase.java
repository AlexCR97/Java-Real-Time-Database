package com.ale.realtime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RealTimeDatabase {

    public static interface OnChangeAllValuesListener {
        public void onChangeAllValues(List<Map<String, Object>> values);
    }
    
    public static interface OnChangeNewValuesListener {
        public void onChangeNewValues(List<Map<String, Object>> values);
    }
    
    public static interface OnChangeOldValuesListener {
        public void onChangeOldValues(List<Map<String, Object>> values);
    }
    
    private final String host;
    private final String port;
    private final String database;
    private final String user;
    private final String password;
    
    private final Map<String, ScheduledFuture> listeningThreads = new HashMap<>();
    private final Map<String, OnChangeAllValuesListener> allValuesListeners = new HashMap<>();
    private final Map<String, OnChangeNewValuesListener> newValuesListeners = new HashMap<>();
    private final Map<String, OnChangeOldValuesListener> oldValuesListeners = new HashMap<>();
    private final Map<String, List<Map<String, Object>>> oldValuesLists = new HashMap<>();

    public RealTimeDatabase(String host, String port, String database, String user, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.password = password;
    }

    public CompletableFuture<Boolean> add(String table, Map<String, Object> values) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection().get()) {
                List<String> columns = values.keySet().stream().collect(Collectors.toList());
                String query = buildAddQuery(table, columns);
                PreparedStatement statement = connection.prepareStatement(query);
                
                int index = 1;
                for (Map.Entry<String, Object> entry : values.entrySet()) {
                    statement.setObject(index++, entry.getValue());
                }
                
                return statement.executeUpdate() > 0;
            }
            catch (SQLException | InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    public CompletableFuture<List<Map<String, Object>>> get(String table) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection().get()) {
                String query = buildGetQuery(table);
                PreparedStatement statement = connection.prepareStatement(query);
                
                ResultSet resultSet = statement.executeQuery();
                ResultSetMetaData metaData = resultSet.getMetaData();
                
                List<Map<String, Object>> objects = new ArrayList<>();
                
                while (resultSet.next()) {
                    Map<String, Object> object = new HashMap<>();
                
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        String key = metaData.getColumnName(i);
                        Object value = resultSet.getObject(i);
                        object.put(key, value);
                    }
                    
                    objects.add(object);
                }
                
                return objects;
            }
            catch (SQLException | InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    public CompletableFuture<Map<String, Object>> get(String table, String idColumn, Object idValue) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection().get()) {
                String query = buildGetQuery(table, idColumn);
                PreparedStatement statement = connection.prepareStatement(query);
                
                statement.setObject(1, idValue);
                
                ResultSet resultSet = statement.executeQuery();
                ResultSetMetaData metaData = resultSet.getMetaData();
                
                Map<String, Object> object = new HashMap<>();
                
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String key = metaData.getColumnName(i);
                    Object value = resultSet.getObject(i);
                    object.put(key, value);
                }
                
                return object;
            }
            catch (SQLException | InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    public CompletableFuture<Boolean> remove(String table, String idColumn, Object idValue) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection().get()) {
                String query = buildRemoveQuery(table, idColumn);
                PreparedStatement statement = connection.prepareStatement(query);
                
                statement.setObject(1, idValue);
                
                return statement.executeUpdate() > 0;
            }
            catch (SQLException | InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    public CompletableFuture<Boolean> update(String table, Map<String, Object> values, String idColumn, Object idValue) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection().get()) {
                List<String> columns = values.keySet().stream().collect(Collectors.toList());
                String query = buildUpdateQuery(table, columns, idColumn);
                PreparedStatement statement = connection.prepareStatement(query);
                
                int index = 1;
                for (Map.Entry<String, Object> entry : values.entrySet()) {
                    statement.setObject(index++, entry.getValue());
                }
                
                statement.setObject(index, idValue);
                
                return statement.executeUpdate() > 0;
            }
            catch (SQLException | InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    public String buildAddQuery(String table, List<String> columns) {
        StringBuilder sb = new StringBuilder();
        
        List<String> placeholders = columns.stream()
                .map(column -> "?")
                .collect(Collectors.toList());
        
        sb.append("INSERT INTO ")
                .append(table)
                .append('(')
                .append(String.join(", ", columns))
                .append(") VALUES (")
                .append(String.join(", ", placeholders))
                .append(')');
        
        return sb.toString();
    }
    
    public String buildGetQuery(String table) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("SELECT * FROM ")
                .append(table);
        
        return sb.toString();
    }
    
    public String buildGetQuery(String table, String idColumn) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("SELECT * FROM ")
                .append(table)
                .append(" WHERE ")
                .append(idColumn)
                .append(" = ?");
        
        return sb.toString();
    }
    
    public String buildRemoveQuery(String table, String idColumn) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("DELETE FROM ")
                .append(table)
                .append(" WHERE ")
                .append(idColumn)
                .append(" = ?");
        
        return sb.toString();
    }
    
    public String buildUpdateQuery(String table, List<String> columns, String idColumn) {
        StringBuilder sb = new StringBuilder();
        
        List<String> placeholders = columns.stream()
                .map(column -> column + " = ?")
                .collect(Collectors.toList());
        
        sb.append("UPDATE ")
                .append(table)
                .append(" SET ")
                .append(String.join(", ", placeholders))
                .append(" WHERE ")
                .append(idColumn)
                .append(" = ?");
        
        return sb.toString();
    }
    
    public CompletableFuture<Connection> getConnection() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String url = getConnectionUrl();
                return DriverManager.getConnection(url, user, password);
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
    
    public void setOnChangeAllValuesListener(String table, OnChangeAllValuesListener allValuesListener) {
        allValuesListeners.put(table, allValuesListener);
    }
    
    public void setOnChangeNewValuesListener(String table, OnChangeNewValuesListener newValuesListener) {
        newValuesListeners.put(table, newValuesListener);
    }
    
    public void setOnChangeOldValuesListener(String table, OnChangeOldValuesListener oldValuesListener) {
        oldValuesListeners.put(table, oldValuesListener);
    }
    
    public void startListening(String table, long delayInMilliseconds) {
        oldValuesLists.put(table, new ArrayList<>());
        
        ScheduledFuture thread = Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                List<Map<String, Object>> allValuesList = get(table).get();
                List<Map<String, Object>> newValuesList = new ArrayList<>();
                
                newValuesList.addAll(allValuesList);
                newValuesList.removeAll(oldValuesLists.get(table));
                
                boolean changeInDatabase = !allValuesList.equals(oldValuesLists.get(table));
                
                if (changeInDatabase) {
                    if (allValuesListeners.containsKey(table)) {
                        allValuesListeners.get(table).onChangeAllValues(allValuesList);
                    }
                    
                    if (newValuesListeners.containsKey(table)) {
                        newValuesListeners.get(table).onChangeNewValues(newValuesList);
                    }
                    
                    if (oldValuesListeners.containsKey(table)) {
                        oldValuesListeners.get(table).onChangeOldValues(oldValuesLists.get(table));
                    }
                }
                
                oldValuesLists.get(table).clear();
                oldValuesLists.get(table).addAll(allValuesList);
            }
            catch (InterruptedException | ExecutionException ex) {
                ex.printStackTrace();
            }
        }, 0, delayInMilliseconds, TimeUnit.MILLISECONDS);
        
        listeningThreads.put(table, thread);
    }
    
    public void stopListening(String table) throws Exception {
        if (!listeningThreads.containsKey(table)) {
            throw new Exception("No listening thread found for table '" + table + "'");
        }
        
        listeningThreads.get(table).cancel(true);
    }
}
