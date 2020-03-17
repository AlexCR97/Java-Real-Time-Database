package com.ale.realtime;

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
        
    }
}
