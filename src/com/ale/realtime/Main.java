package com.ale.realtime;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Main {
    
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        
        RealTimeDatabase db = new RealTimeDatabase(
                "localhost",
                "3306",
                "realtime_db",
                "root",
                "1234"
        );
        
        System.out.println("Getting maps...");
        db.getAll("users").get().forEach(System.out::println);
        System.out.println("");
        
        System.out.println("Getting POJOs...");
        db.getAll("users", User.class).get().forEach(System.out::println);
        System.out.println("");
        
        System.out.println("Getting map where...");
        System.out.println(db.getWhere("users", "id", 5).get());
        System.out.println("");
        
        System.out.println("Getting POJO where...");
        System.out.println(db.getWhere("users", "id", 5, User.class).get());
        System.out.println("");
        
        System.out.println("Reading maps...");
        db.readQuery("select * from users").get().forEach(System.out::println);
        System.out.println("");
        
        System.out.println("Reading POJOs...");
        db.readQuery("select * from users", User.class).get().forEach(System.out::println);
        System.out.println("");
        
        Map<String, Object> map;
        
        System.out.println("Statement maps...");
        map = new HashMap<>();
        map.put("id", 3);
        db.readStatement("select * from users where id = ?", map).get().forEach(System.out::println);
        System.out.println("");
        
        System.out.println("Statement POJOs...");
        map = new HashMap<>();
        map.put("id", 3);
        db.readStatement("select * from users where id = ?", map, User.class).get().forEach(System.out::println);
        System.out.println("");
    }
}
