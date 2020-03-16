package com.ale.realtime;

import java.sql.Connection;
import java.util.concurrent.ExecutionException;

public class Main {
    
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        
        RealTimeDatabase db = new RealTimeDatabase(
                "remotemysql.com",
                "3306",
                "XtRKorjMr6",
                "XtRKorjMr6",
                "smL3J59hNk"
        );
        
        System.out.println("Getting connection...");
        
        Connection conn = db.getConnection().get();
        
        if (conn == null) {
            System.out.println("Connection could not be established :(");
            return;
        }
        
        db.startListening("users", User.class, 1000);
        System.out.println("Listening table users...");
        
        db.setOnChangeAllValuesListener("users", User.class, allValues -> {
            System.out.println("Change in database! All values are:");
            allValues.forEach(System.out::println);
            System.out.println("===================================");
        });
        
        db.setOnChangeNewValuesListener("users", User.class, newValues -> {
            System.out.println("Change in database! New values are:");
            newValues.forEach(System.out::println);
            System.out.println("===================================");
        });
        
        db.setOnChangeOldValuesListener("users", User.class, oldValues -> {
            System.out.println("Change in database! Old values are:");
            oldValues.forEach(System.out::println);
            System.out.println("===================================");
        });
    }
}
