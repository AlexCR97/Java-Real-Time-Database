package com.ale.realtime;

public class Main {
    
    public static void main(String[] args) {
        
        RealTimeDatabase db = new RealTimeDatabase(
                "localhost",
                "3306",
                "realtime_db",
                "root",
                "1234"
        );
        
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
