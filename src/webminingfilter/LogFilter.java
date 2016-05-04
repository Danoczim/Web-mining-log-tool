/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package webminingfilter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Kušický
 */
public class LogFilter {

    File file;
    String delimiter;
    int counter = 0;

    ArrayList<String> unfilteredLog = new ArrayList<>();
    ArrayList<Object> columnsList = new ArrayList<>();

    String[] fileTypes = new String[]{
        ".js", ".jpg", ".css", ".png", ".flv", ".gif", ".ico", ".jpeg", ".swf", ".rss", ".xml", ".cur"
    };

    String[] week = new String[]{"05/Dec/2011", "06/Dec/2011", "07/Dec/2011", "08/Dec/2011", "09/Dec/2011", "10/Dec/2011", "11/Dec/2011"};

    FilterListener listener;

    public LogFilter(File file, String delimiter) {
        this.file = file;
        this.delimiter = delimiter;
    }

    public void setOnFilterListener(FilterListener listener) {
        this.listener = listener;
    }

    public void loadFile() {
        listener.onStart();
        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    BufferedReader buffReader = new BufferedReader(new FileReader(file));
                    String line = null;

                    while ((line = buffReader.readLine()) != null) {
                        listener.onUpdate(0, "Loading file...");
                        for (String day : week) {
                            if (line.contains(day)) {
                                unfilteredLog.add(line);
                            }

                        }

                    }

                    listener.onUpdate(unfilteredLog.size(), "Filtering file extensions...");

                    filterExtensions();

                    listener.onUpdate(unfilteredLog.size(), "Filtering status codes...");

                    divideToColumnsForTable(delimiter);

                    listener.onUpdate(columnsList.size(), "Filtering status codes and methods...");

                    filterStatusCodesAndMethods();
                    
                    listener.onUpdate(columnsList.size(), "Filtering robots...");

                    filterRobots();
                    
                    listener.onUpdate(columnsList.size(), "Creating table...");
                    listener.onFinish(columnsList.size(), columnsList);

                } catch (FileNotFoundException ex) {
                    Logger.getLogger(LogFilter.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex) {
                    Logger.getLogger(LogFilter.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }).start();
    }

    private ArrayList<Object> divideToColumnsForTable(String delimiter) {
        columnsList = new ArrayList();
        Iterator iterator = unfilteredLog.iterator();

        while (iterator.hasNext()) {
            String row = (String) iterator.next();
            String[] parts = row.split(delimiter);

            //Object[] objects = new Object[parts.length];
            Object[] objects = new Object[12];

            for (int j = 0; j < parts.length; j++) {
                if (j < 12) {
                    objects[j] = parts[j];
                } else {
                    objects[11] = (String) objects[11] + "" + parts[j];
                }

            }

            columnsList.add(objects);
        }

        unfilteredLog = null;
        System.gc();
        return columnsList;
    }

    private void filterExtensions() {
        Iterator iterator = unfilteredLog.iterator();
        while (iterator.hasNext()) {
            String line = (String) iterator.next();

            fileTypeLoop:
            for (String fileType : fileTypes) {
                if (line.contains(fileType)) {
                    iterator.remove();

                    break fileTypeLoop;
                }
            }
        }
    }

    private void filterStatusCodesAndMethods() {
        Iterator iterator = columnsList.iterator();

        while (iterator.hasNext()) {
            Object[] row = (Object[]) iterator.next();
            String statusCode = (String) row[8];
            String method = (String) row[5];

            if (statusCode.startsWith("4") || statusCode.startsWith("5") || !method.contains("GET")) {
                iterator.remove();
            }

        }
    }

    private void filterRobots() {
        ArrayList<String> robotsIP = new ArrayList();

        for (int i = 0; i < columnsList.size(); i++) {
            Object[] row = (Object[]) columnsList.get(i);
            String robotsTxt = (String) row[6];
            String ip = (String) row[0];
            if (robotsTxt.contains("robots.txt")) {
                {
                    robotsIP.add(ip);
                }

            }
        }
        
        Iterator iterator = columnsList.iterator();
        
        while (iterator.hasNext()){
            String row = (String) ((Object[]) iterator.next())[0];
            
            robotsLoop:
            for (String robotIp : robotsIP){
                if (row.equals(robotIp)){
                    iterator.remove();
                    break robotsLoop;
                }
            }
        }
    }

}
