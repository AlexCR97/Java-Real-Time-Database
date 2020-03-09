package com.ale.realtime;

public class TableListenerException extends Exception {

    public TableListenerException(String table) {
        super("Error encountered while listening for changes on table '" + table + "'");
    }
    
}
