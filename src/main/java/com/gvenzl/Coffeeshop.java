package com.gvenzl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

public class Coffeeshop {

    private final Properties props;

    public static void main(String[] args) throws Exception {

        if (args.length > 0) {
            printHelp();
            System.exit(0);
        }

        try {
			new Coffeeshop().run();
		} catch (IOException e) {
            System.out.println("'Coffeeshop.properties' file could not be found in working directory.");
            System.out.println("Please provide the properties file and make sure it's readable.");
        }
    }

    public Coffeeshop() throws IOException {
        props = new Properties();
        props.load(new FileInputStream("Coffeeshop.properties"));
    }

    private void run() throws Exception {

        ArrayList<Thread> threads = new ArrayList<>();
        for (int i=0; i < Integer.valueOf(props.getProperty("threads")); i++) {
            Thread t = new Thread(new Worker(props));
            t.start();
            threads.add(t);
        }

        for (Thread t : threads) {
            t.join();
        }
    }

    private static void printHelp() {

        System.out.println("Usage: java -jar coffeeshop.jar");
        System.out.println();
        System.out.println("This program generates coffeeshop data.");
        System.out.println();
        System.out.println("You can load data either via RESTful web services using ORDS, " +
                           "a Cloud Connection credentials file, or a regular JDBC connect string. " +
                           "Alternatively, you can also generate a file with the data");
        
    }


}
