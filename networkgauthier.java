package main.java;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by Gauthier on 02/11/2016.
 */
public class networkgauthier {

    private static Configuration conf = null;
    static{
        conf = HBaseConfiguration.create();
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    }

    /**
     * We first create a table
     */

    public static void createTable(String tableName, String[] families)
            throws Exception {

        HBaseAdmin admin = new HBaseAdmin(conf);


        HTableDescriptor tableDesc = new HTableDescriptor(tableName);

        for (int i = 0; i < families.length; i++) {
            tableDesc.addFamily(new HColumnDescriptor(families[i]));
        }
        admin.createTable(tableDesc);
        System.out.println("Table"+ tableName + "was sucessfully created.");
    }


    /**
     * Row insertion
     */

    public static void Record(String tableName, String Key,
                                 String family, String qualifier, String value) throws Exception {
        try {

            HTable table = new HTable(conf, tableName);

            Put put = new Put(Bytes.toBytes(Key));

            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("Record" + Key + "was successfully added to"
                    + tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * My main
     */

    public static void main(String[] args) throws Exception {
        ArrayList<String> friendsArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        boolean g = true;
        boolean d = true;
        String value;
        String yesNo = "";
        String firstName;
        Scanner sc = new Scanner(System.in);
        String tableName = "gdamiennetwork";
        String[] columnFamilies = {"info", "friends"};

        // We first create a table
        networkgauthier.createTable(tableName, columnFamilies);

        // Then we enter all the information
        do {
            // The ID
            System.out.println("Welcome, please enter a BFFID:");
            firstName = sc.nextLine();


            // The age
            System.out.println("Now your age:");
            value = sc.nextLine();
            networkgauthier.Record(tableName, firstName, "info", "age", value);

            // The gender
            System.out.println("Your gender (M or F) : ");
            value = sc.nextLine();
            networkgauthier.Record(tableName, firstName, "info", "gender", value);

            // The BFF
            System.out.println("Who is your BFF ?");
            value = sc.nextLine();
            networkgauthier.Record(tableName, firstName, "friends", "BFF", value);

            // Now we aad other friends if needed
            do {
                System.out.println("Do you have other friends ? y or n");
                yesNo = sc.nextLine();
                while (yesNo.charAt(0) != 'n' && yesNo.charAt(0) != 'y') {
                    System.out.println("I did not understand. y or n ?");
                    yesNo = sc.nextLine();
                }

                if(yesNo.charAt(0) == 'n') {
                    d = false;
                    break;
                }

                // We insert the friends in an array and convert it into strings
                System.out.println("What's the name of your friend ?");
                value = sc.nextLine();

                friendsArray.add(value);
            }while(d);

            for(String s : friendsArray)
            {
                sb.append(s);
                sb.append(", ");
            }

            networkgauthier.Record(tableName, firstName, "friends", "others", sb.toString());

            // Finally we loop over again if the user wants to add more friends
            System.out.println("Do you want to add a new person's info in the table ? y or n");
            yesNo = sc.nextLine();
            while(yesNo.charAt(0) != 'n' && yesNo.charAt(0) != 'y')
            {
                System.out.println("I did not understand. y or n ?");
                yesNo = sc.nextLine();
            }

            if(yesNo.charAt(0) == 'n')
                g = false;
        }while(g);
        System.out.println("See you soon!");
    }
}
