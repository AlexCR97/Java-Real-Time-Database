package com.ale.realtime;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    
    public static void main(String[] args) {
        
        /*Map<String, Object> map = new HashMap<>();
        map.put("id", 1);
        map.put("email", "alex@live.com");
        map.put("password", "1234");
        
        Gson gson = new Gson();
        JsonElement json = gson.toJsonTree(map);
        User user = gson.fromJson(json, User.class);
        
        System.out.println(user);*/
        
        RealTimeDatabase db = new RealTimeDatabase(
                "localhost",
                "3306",
                "realtime_db",
                "root",
                "1234"
        );
        
        User user = new User();
        user.setEmail("jayson@outlook.com");
        user.setPassword("123456");
        
        try {
            /*System.out.println("Adding user...");
            db.add("users", user).get();*/
        
            System.out.println("Getting users...");
            db.get("users", User.class).get().forEach(System.out::println);
            
            /*System.out.println("Getting user...");
            System.out.println(db.get("users", "id", 5, User.class).get());*/
        }
        catch (InterruptedException | ExecutionException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        /*db.startListening("users", 1000);
        System.out.println("Listening table users...");*/

        /*db.setOnChangeAllValuesListener("users", allValues -> {
        System.out.println("Change in database! All values are:");
        allValues.forEach(System.out::println);
        });*/

        /*db.setOnChangeNewValuesListener("users", newValues -> {
        System.out.println("Change in database! New values are:");
        newValues.forEach(System.out::println);
        });*/

        /*db.setOnChangeOldValuesListener("users", oldValues -> {
        System.out.println("Change in database! Old values are:");
        oldValues.forEach(System.out::println);
        });*/

        /*try {
        db.stopListening("users");
        System.out.println("Stopped listening table users");
        }
        catch (Exception ex) {
        Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }*/
    }
}
