package com.ale.realtime;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
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
    
    /**
     * Represents the operation executed when a new change is given on the listened table.
     * This interface takes into account all the records within the table.
     * @param <T> The type that represents the entities within the table in the database
     */
    @FunctionalInterface
    public static interface OnChangeAllValuesListener<T extends Object> {
        /**
         * Executes this operation with the given list of objects
         * @param values All the values within the table
         */
        public void onChangeAllValues(List<T> values);
    }
    
    /**
     * Represents the operation executed when a new change is given on the listened table.
     * This interface only takes into account the new records within the table.
     * @param <T> The type that represents the entities within the table in the database
     */
    @FunctionalInterface
    public static interface OnChangeNewValuesListener<T extends Object> {
        /**
         * Executes this operation with the given list of objects
         * @param values The new values within the table
         */
        public void onChangeNewValues(List<T> values);
    }
    
    /**
     * Represents the operation executed when a new change is given on the listened table.
     * This interface only takes into account the old records within the table.
     * @param <T> The type that represents the entities within the table in the database
     */
    @FunctionalInterface
    public static interface OnChangeOldValuesListener<T extends Object> {
        /**
         * Executes this operation with the given list of objects
         * @param values The old values within the table
         */
        public void onChangeOldValues(List<T> values);
    }
    
    private final DatabaseType databaseType;
    private final String host;
    private final String port;
    private final String database;
    private final String user;
    private final String password;
    
    private final Gson gson = new Gson();
    private final Map<String, ScheduledFuture> listeningThreads = new HashMap<>();
    private final Map<String, OnChangeAllValuesListener> allValuesListeners = new HashMap<>();
    private final Map<String, OnChangeNewValuesListener> newValuesListeners = new HashMap<>();
    private final Map<String, OnChangeOldValuesListener> oldValuesListeners = new HashMap<>();
    private final Map<String, List> oldValuesLists = new HashMap<>();
    
    /**
     * Creates a new instance for a real time database
     * 
     * @param databaseType The database engine to be used
     * @param host The server on which the database is located
     * @param port The port on which the server is listening
     * @param database The name of the database
     * @param user The user to log into the server
     * @param password The password to log into the server
     */
    public RealTimeDatabase(DatabaseType databaseType, String host, String port, String database, String user, String password) {
        this.databaseType = databaseType;
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.password = password;
    }
    
    /**
     * Gets all the records from the specified table (performs a SELECT operation)
     * @param table The name of the table from which to select data
     * @return A future what will return a list containing maps that represent the entities of the table
     */
    public CompletableFuture<List<Map<String, Object>>> getAll(String table) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection().get()) {
                
                String query = buildGetQuery(table);
                PreparedStatement statement = connection.prepareStatement(query);
                ResultSet resultSet = statement.executeQuery();
                
                return resultSetToMapList(resultSet);
            }
            catch (SQLException | InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    /**
     * Gets all the records from the specified table (performs a SELECT operation)
     * @param <T> The type that represents the entities within the table in the database
     * @param table The name of the table from which to select data
     * @param clazz The POJO class to which convert the table records
     * @return A future what will return a list containing POJOs that represent the entities of the table
     */
    public <T extends Object> CompletableFuture<List<T>> getAll(String table, Class<T> clazz) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<Map<String, Object>> maps = getAll(table).get();
                return mapListToPojoList(maps, clazz);
            }
            catch (InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    /**
     * Gets the record from the specified table that matches the given id (performs a SELECT WHERE operation)
     * @param table The name of the table from which to select data
     * @param idColumn The column name the query will use to filter
     * @param idValue The value corresponding to the given column name
     * @return A future that will return a map representing the selected entity from the table
     */
    public CompletableFuture<Map<String, Object>> getWhere(String table, String idColumn, Object idValue) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection().get()) {
                String query = buildGetQuery(table, idColumn);
                
                PreparedStatement statement = connection.prepareStatement(query);
                statement.setObject(1, idValue);
                
                ResultSet resultSet = statement.executeQuery();
                List<Map<String, Object>> maps = resultSetToMapList(resultSet);
                
                return maps.get(0);
            }
            catch (SQLException | InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    /**
     * Gets the record from the specified table that matches the given id (performs a SELECT WHERE operation)
     * @param <T> The type that represents the entities within the table in the database
     * @param table The name of the table from which to select data
     * @param idColumn The column name the query will use to filter
     * @param idValue The value corresponding to the given column name
     * @param clazz The POJO class to which convert the table record
     * @return A future that will return a POJO representing the selected entity from the table
     */
    public <T extends Object> CompletableFuture<T> getWhere(String table, String idColumn, Object idValue, Class<T> clazz) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Map<String, Object> map = getWhere(table, idColumn, idValue).get();
                return mapToPojo(map, clazz);
            }
            catch (InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    /**
     * Executes a custom read query. It is expected to get a list of table records from this method.
     * @param query The SQL query to be executed
     * @return A future what will return a list containing maps that represent the entities of the table
     */
    public CompletableFuture<List<Map<String, Object>>> readQuery(String query) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection().get()) {
                PreparedStatement statement = connection.prepareStatement(query);
                ResultSet resultSet = statement.executeQuery();
                return resultSetToMapList(resultSet);
            }
            catch (SQLException | InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    /**
     * Executes a custom read query. It is expected to get a list of table records from this method.
     * @param <T> The type that represents the entities within the table in the database
     * @param query The SQL query to be executed
     * @param clazz The POJO class to which convert the table records
     * @return A future what will return a list containing objects that represent the entities of the table
     */
    public <T extends Object> CompletableFuture<List<T>> readQuery(String query, Class<T> clazz) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<Map<String, Object>> maps = readQuery(query).get();
                return mapListToPojoList(maps, clazz);
            }
            catch (InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    /**
     * Executes a custom prepared statement. It is expected to get a list of table records from this method.
     * @param query The SQL query to be executed
     * @param values The values of the prepared statement
     * @return A future what will return a list containing maps that represent the entities of the table
     */
    public CompletableFuture<List<Map<String, Object>>> readStatement(String query, Map<String, Object> values) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection().get()) {
                PreparedStatement statement = connection.prepareStatement(query);
                
                int index = 1;
                for (Map.Entry<String, Object> entry : values.entrySet()) {
                    statement.setObject(index++, entry.getValue());
                }
                
                ResultSet resultSet = statement.executeQuery();
                return resultSetToMapList(resultSet);
            }
            catch (SQLException | InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    /**
     * Executes a custom prepared statement. It is expected to get a list of table records from this method.
     * @param <T> The type that represents the entities within the table in the database
     * @param query The SQL query to be executed
     * @param values The values of the prepared statement
     * @param clazz The POJO class to which convert the table records
     * @return A future what will return a list containing objects that represent the entities of the table
     */
    public <T extends Object> CompletableFuture<List<T>> readStatement(String query, Map<String, Object> values, Class<T> clazz) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<Map<String, Object>> maps = readStatement(query, values).get();
                return mapListToPojoList(maps, clazz);
            }
            catch (InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    /**
     * Adds the provided entity to the database (performs an INSERT operation)
     * @param table The name of the table in which to insert the data
     * @param map A map used to represent the entity
     * @return A future that will return a boolean, indicating whether the operation was successful or not
     */
    public CompletableFuture<Boolean> add(String table, Map<String, Object> map) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection().get()) {
                List<String> columnNames = map.keySet().stream().collect(Collectors.toList());
                String query = buildAddQuery(table, columnNames);
                PreparedStatement statement = connection.prepareStatement(query);
                
                int index = 1;
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    statement.setObject(index++, entry.getValue());
                }
                
                return statement.executeUpdate() > 0;
            }
            catch (SQLException | InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    /**
     * Adds the provided entity to the database (performs an INSERT operation)
     * @param <T> The type that represents the entities within the table in the database
     * @param table The name of the table in which to insert the data
     * @param object The POJO (Plain Old Java Object) used to represent the entity
     * @return A future that will return a boolean, indicating whether the operation was successful or not
     */
    public <T extends Object> CompletableFuture<Boolean> add(String table, T object) {
        Map<String, Object> map = pojoToMap(object);
        return add(table, map);
    }
    
    /**
     * Removes the entity which has the given column-value from the table (performs a DELETE WHERE operation)
     * @param table The name of the table from which to delete data
     * @param idColumn The column name the query will use to filter
     * @param idValue The value corresponding to the given column name
     * @return A future that will return a boolean, indicating whether the operation was successful or not
     */
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
    
    /**
     * Updates the entity which has the given column-value from the table (performs UPDATE WHERE operation)
     * @param table The name of the table from which to delete data
     * @param map A map used to represent the entity
     * @param idColumn The column name the query will use to filter
     * @param idValue The value corresponding to the given column name
     * @return A future that will return a boolean, indicating whether the operation was successful or not
     */
    public CompletableFuture<Boolean> update(String table, Map<String, Object> map, String idColumn, Object idValue) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection().get()) {
                List<String> columnNames = map.keySet().stream().collect(Collectors.toList());
                String query = buildUpdateQuery(table, columnNames, idColumn);
                PreparedStatement statement = connection.prepareStatement(query);
                
                int index = 1;
                for (Map.Entry<String, Object> entry : map.entrySet()) {
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
    
    /**
     * Updates the entity which has the given column-value from the table (performs UPDATE WHERE operation)
     * @param <T> The type that represents the entities within the table in the database
     * @param table The name of the table from which to delete data
     * @param object The POJO (Plain Old Java Object) used to represent the entity
     * @param idColumn The column name the query will use to filter
     * @param idValue The value corresponding to the given column name
     * @return A future that will return a boolean, indicating whether the operation was successful or not
     */
    public <T extends Object> CompletableFuture<Boolean> update(String table, T object, String idColumn, Object idValue) {
        Map<String, Object> map = pojoToMap(object);
        return update(table, map, idColumn, idValue);
    }
    
    /**
     * Executes a custom write query. It is expected that this method causes a change in the database.
     * @param query The SQL query to be executed
     * @return A future that will return a boolean, indicating whether the operation was successful or not
     */
    public CompletableFuture<Boolean> writeQuery(String query) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection().get()) {
                PreparedStatement statement = connection.prepareStatement(query);
                return statement.executeUpdate() > 0;
            }
            catch (SQLException | InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    /**
     * Executes a custom write query. It is expected that this method causes a change in the database.
     * @param query The SQL query to be executed
     * @param values The values of the prepared statement
     * @return A future that will return a boolean, indicating whether the operation was successful or not
     */
    public CompletableFuture<Boolean> writeStatement(String query, List<Object> values) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection connection = getConnection().get()) {
                PreparedStatement statement = connection.prepareStatement(query);
                
                int index = 1;
                for (Object value : values) {
                    statement.setObject(index++, value);
                }
                
                return statement.executeUpdate() > 0;
            }
            catch (SQLException | InterruptedException | ExecutionException ex) {
                throw new CompletionException(ex);
            }
        });
    }
    
    /**
     * Generates an INSERT query
     * @param table The name of the table
     * @param columnNames The names of the columns
     * @return A string that represents an INSERT query, given the table name and the columns
     */
    private String buildAddQuery(String table, List<String> columnNames) {
        StringBuilder sb = new StringBuilder();
        
        List<String> placeholders = columnNames.stream()
                .map(column -> "?")
                .collect(Collectors.toList());
        
        sb.append("INSERT INTO ")
                .append(table)
                .append('(')
                .append(String.join(", ", columnNames))
                .append(") VALUES (")
                .append(String.join(", ", placeholders))
                .append(')');
        
        return sb.toString();
    }
    
    /**
     * Generates a SELECT query
     * @param table The name of the table
     * @return A string that represents a SELECT query, given the table name
     */
    public String buildGetQuery(String table) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("SELECT * FROM ")
                .append(table);
        
        return sb.toString();
    }
    
    /**
     * Generates a SELECT WHERE query
     * @param table The name of the table
     * @param idColumn The id column used for the WHERE clause
     * @return A string that represents a SELECT WHERE query, given the table name
     */
    public String buildGetQuery(String table, String idColumn) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("SELECT * FROM ")
                .append(table)
                .append(" WHERE ")
                .append(idColumn)
                .append(" = ?");
        
        return sb.toString();
    }
    
    /**
     * Generates a DELETE WHERE query
     * @param table The name of the table
     * @param idColumn The id column used for the WHERE clause
     * @return A string that represents a DELETE WHERE query
     */
    public String buildRemoveQuery(String table, String idColumn) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("DELETE FROM ")
                .append(table)
                .append(" WHERE ")
                .append(idColumn)
                .append(" = ?");
        
        return sb.toString();
    }
    
    /**
     * Generates an UPDATE WHERE query
     * @param table The name of the table
     * @param columnNames The names of the columns
     * @param idColumn The id column used for the WHERE clause
     * @return A string that represents an UPDATE WHERE clause
     */
    public String buildUpdateQuery(String table, List<String> columnNames, String idColumn) {
        StringBuilder sb = new StringBuilder();
        
        List<String> placeholders = columnNames.stream()
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
    
    /**
     * Gets the connection object for the corresponding database
     * @return The connection object
     */
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
    
    /**
     * Gets the connection URL for the corresponding database
     * @return The URL string
     */
    public String getConnectionUrl() {
        if (databaseType.equals(DatabaseType.MYSQL)) {
            return String.format(
                    "jdbc:mysql://%s:%s/%s?"
                            + "useUnicode=true&"
                            + "useJDBCCompliantTimezoneShift=true&"
                            + "useLegacyDatetimeCode=false&"
                            + "serverTimezone=UTC",
                    host, port, database
            );
        }
        else if (databaseType.equals(DatabaseType.POSTGRESQL)) {
            return String.format("jdbc:postgresql://%s:%s/%s", host, port, database);
        }
        else {
            return "";
        }
    }
    
    /**
     * Sets the listener to be triggered when changes on the listened table occur.
     * The data provided for this listener are all the values from the table.
     * @param table The name of the table
     * @param allValuesListener The listener
     */
    public void setOnChangeAllValuesListener(String table, OnChangeAllValuesListener<Map<String, Object>> allValuesListener) {
        allValuesListeners.put(table, allValuesListener);
    }
    
    /**
     * Sets the listener to be triggered when changes on the listened table occur.
     * The data provided for this listener are all the values from the table.
     * @param <T> The type that represents the entities within the table in the database
     * @param table The name of the table
     * @param clazz The POJO class to which convert the table record
     * @param allValuesListener The listener
     */
    public <T extends Object> void setOnChangeAllValuesListener(String table, Class<T> clazz, OnChangeAllValuesListener<T> allValuesListener) {
        allValuesListeners.put(table, allValuesListener);
    }
    
    /**
     * Sets the listener to be triggered when changes on the listened table occur.
     * The data provided for this listener are the new values from the table.
     * @param table The name of the table
     * @param newValuesListener The listener
     */
    public void setOnChangeNewValuesListener(String table, OnChangeNewValuesListener<Map<String, Object>> newValuesListener) {
        newValuesListeners.put(table, newValuesListener);
    }
    
    /**
     * Sets the listener to be triggered when changes on the listened table occur.
     * The data provided for this listener are the new values from the table.
     * @param <T> The type that represents the entities within the table in the database
     * @param table The name of the table
     * @param clazz The POJO class to which convert the table record
     * @param newValuesListener The listener
     */
    public <T extends Object> void setOnChangeNewValuesListener(String table, Class<T> clazz, OnChangeNewValuesListener<T> newValuesListener) {
        newValuesListeners.put(table, newValuesListener);
    }
    
    /**
     * Sets the listener to be triggered when changes on the listened table occur.
     * The data provided for this listener are the old values from the table.
     * @param table The name of the table
     * @param oldValuesListener The listener
     */
    public void setOnChangeOldValuesListener(String table, OnChangeOldValuesListener<Map<String, Object>> oldValuesListener) {
        oldValuesListeners.put(table, oldValuesListener);
    }
    
    /**
     * Sets the listener to be triggered when changes on the listened table occur.
     * The data provided for this listener are the old values from the table.
     * @param <T> The type that represents the entities within the table in the database
     * @param table The name of the table
     * @param clazz The POJO class to which convert the table record
     * @param oldValuesListener The listener
     */
    public <T extends Object> void setOnChangeOldValuesListener(String table, Class<T> clazz, OnChangeOldValuesListener oldValuesListener) {
        oldValuesListeners.put(table, oldValuesListener);
    }
    
    /**
     * Observes for changes in the specified table every N milliseconds.
     * When any change in the table is detected, this method triggers the changes listeners
     * and provides the data changes.
     * @param table The name of the table
     * @param delayInMilliseconds The interval in which to listen to changes
     */
    public void startListening(String table, long delayInMilliseconds) {
        oldValuesLists.put(table, new ArrayList<>());
        
        ScheduledFuture thread = Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                List<Map<String, Object>> allValuesList = getAll(table).get();
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
    
    /**
     * Observes for changes in the specified table every N milliseconds.
     * When any change in the table is detected, this method triggers the changes listeners
     * and provides the data changes.
     * @param <T> The type that represents the entities within the table in the database
     * @param table The name of the table
     * @param clazz The POJO class to which convert the table records
     * @param delayInMilliseconds The interval in which to listen to changes
     */
    public <T extends Object> void startListening(String table, Class<T> clazz, long delayInMilliseconds) {
        oldValuesLists.put(table, new ArrayList<>());
        
        ScheduledFuture thread = Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                List<T> allValuesList = getAll(table, clazz).get();
                List<T> newValuesList = new ArrayList<>();
                
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
    
    /**
     * Stops checking for changes in the specified table. If no change listeners have been
     * setup for the table, this method does nothing.
     * @param table The name of the table
     */
    public void stopListening(String table) {
        if (!listeningThreads.containsKey(table)) {
            return;
        }
        
        listeningThreads.get(table).cancel(true);
    }
    
    /**
     * Parses a map into a POJO
     * @param <T> The type into which the map will be parsed
     * @param map The map used to represent the POJO
     * @param clazz The class of the POJO
     * @return A POJO equivalent to the map representation
     */
    private <T extends Object> T mapToPojo(Map<String, Object> map, Class<T> clazz) {
        JsonElement json = gson.toJsonTree(map);
        T object = gson.fromJson(json, clazz);
        return object;
    }
    
    /**
     * Parses a whole list of maps into a list of POJOs
     * @param <T> The type into which the map will be parsed
     * @param map The map used to represent the POJO
     * @param clazz The class of the POJO
     * @return A list containing all the POJO equivalents of the maps in the provided list
     */
    private <T extends Object> List<T> mapListToPojoList(List<Map<String, Object>> maps, Class<T> clazz) {
        List<T> objects = new ArrayList<>();
        
        maps.forEach(map -> {
            T object = mapToPojo(map, clazz);
            objects.add(object);
        });
        
        return objects;
    }
    
    /**
     * Parses a POJO into a map
     * @param <T> The type from which the map will be parsed
     * @param object The POJO
     * @return A map equivalent to the POJO representation
     */
    private <T extends Object> Map<String, Object> pojoToMap(T object) {
        String json = gson.toJson(object);
        Map<String, Object> map = gson.fromJson(json, Map.class);
        return map;
    }
    
    /**
     * Parses a SQL Result Set into a list of maps
     * @param resultSet The result set to be parsed
     * @return A list containing all the map equivalents of the records in the result set
     * @throws SQLException
     */
    private List<Map<String, Object>> resultSetToMapList(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        List<Map<String, Object>> maps = new ArrayList<>();
        
        while (resultSet.next()) {
            Map<String, Object> map = new HashMap<>();

            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String key = metaData.getColumnName(i);
                Object value = resultSet.getObject(i);
                map.put(key, value);
            }

            maps.add(map);
        }
        
        return maps;
    }
    
    /**
     * Parses a SQL Result Set into a list of objects
     * @param <T> The type from which the map will be parsed
     * @param resultSet The result set to be parsed
     * @param clazz The class of the POJO
     * @return A list containing all the POJO equivalents of the maps in the provided list
     * @throws SQLException 
     */
    private <T extends Object> List<T> resultSetToPojoList(ResultSet resultSet, Class<T> clazz) throws SQLException {
        List<Map<String, Object>> maps = resultSetToMapList(resultSet);
        return mapListToPojoList(maps, clazz);
    }
}

