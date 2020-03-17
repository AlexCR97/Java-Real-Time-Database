package com.ale.realtime;

import java.util.concurrent.ExecutionException;

public class Main {
    
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        
        RealTimeDatabase db = new RealTimeDatabase(
                DatabaseType.POSTGRESQL,
                "localhost",
                "5432",
                "realtime_db",
                "postgres",
                "1234"
        );
        
        System.out.println("Getting PostgresSQL connection");
        db.getConnection().get();
        System.out.println("Success!");
        System.out.println("");
        
        db.getAll("users").get().forEach(System.out::println);
        System.out.println("");
        
        db.getAll("users", User.class).get().forEach(System.out::println);
        System.out.println("");
    }
}
