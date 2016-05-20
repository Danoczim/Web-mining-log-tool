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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Danóczi & Kušický :D
 */
public class LogFilter {

    private final File file;
    private final String delimiter;
    private final boolean filterDates;

    private int URLColumnNumber;
    private int methodColumnNumber;
    private int statusCodeColumnNumber;
    private int agentColumnNumber;
    private int dateColumnNumber;

    private final UIFilterListener listener;
    private ArrayList<String> unfilteredLog = new ArrayList<>();
    private ArrayList<Object[]> dataList;

    private final String[] fileTypes = new String[]{
        ".js",
        ".jpg",
        ".css",
        ".png",
        ".flv",
        ".gif",
        ".ico",
        ".jpeg",
        ".swf",
        ".rss",
        ".xml",
        ".cur"
    };

    private String[] week = new String[]{
        "05/Dec/2011",
        "06/Dec/2011",
        "07/Dec/2011",
        "08/Dec/2011",
        "09/Dec/2011",
        "10/Dec/2011",
        "11/Dec/2011"
    };

    public LogFilter(File file, String delimiter, boolean filterDates, UIFilterListener filterListener) {
        this.file = file;
        this.delimiter = delimiter;
        this.filterDates = filterDates;
        this.listener = filterListener;
        this.URLColumnNumber = this.methodColumnNumber = this.statusCodeColumnNumber = this.agentColumnNumber = this.dateColumnNumber = -1;
    }

    public void setURLColumnNumber(int URLColumnNumber) {
        this.URLColumnNumber = URLColumnNumber - 1;
    }

    public void setStatusCodeColumnNumber(int statusCodeColumnNumber) {
        this.statusCodeColumnNumber = statusCodeColumnNumber - 1;
    }

    public void setMethodColumnNumber(int methodColumnNumber) {
        this.methodColumnNumber = methodColumnNumber - 1;
    }

    public void setAgentColumnNumber(int agentColumnNumber) {
        this.agentColumnNumber = agentColumnNumber - 1;
    }
    
    public void setDateColumnNumber(int dateColumnNumber){
        this.dateColumnNumber = dateColumnNumber -1;
    }

    public void filterFile() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                if (dataList == null) {
                    listener.onFilterFileError("You have to load file first!");
                    return;
                }
                if (URLColumnNumber == -1) {
                    listener.onFilterFileError("Provide URL column number!");
                    return;
                }
                if (statusCodeColumnNumber == -1) {
                    listener.onFilterFileError("Provide status code column number!");
                    return;
                }
                if (methodColumnNumber == -1) {
                    listener.onFilterFileError("Provide method column number!");
                    return;
                }

                filterExtensions(dataList);

                listener.onFinish(dataList.size(), dataList);

                filterStatusCodesAndMethods(dataList);

                listener.onFinish(dataList.size(), dataList);

                filterRobots(dataList);
                
                try {
                    generateUnixTime(dataList);
                } catch (ParseException ex) {
                    Logger.getLogger(LogFilter.class.getName()).log(Level.SEVERE, null, ex);
                }
                listener.onFinish(dataList.size(), dataList);
            }
        }).start();
    }

    public void loadFile() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    BufferedReader buffReader = new BufferedReader(new FileReader(file));
                    String line;
                    int numberOfAllLines = 0;
                    int actualLine = 0;

                    while ((line = buffReader.readLine()) != null) {
                        numberOfAllLines++;
                        if (filterDates) {
                            for (String day : week) {
                                if (line.contains(day)) {
                                    unfilteredLog.add(line);
                                    actualLine++;
                                    listener.onUpdate(actualLine, "Loading file...");
                                }
                            }
                        } else {
                            unfilteredLog.add(line);
                            actualLine++;
                            listener.onUpdate(actualLine, "Loading file...");
                        }
                    }
                    listener.onFirstLoad(numberOfAllLines);

                    dataList = divideToColumnsForTable(unfilteredLog, delimiter);

                    listener.onFinish(dataList.size(), dataList);
                } catch (FileNotFoundException ex) {
                    Logger.getLogger(LogFilter.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex) {
                    Logger.getLogger(LogFilter.class.getName()).log(Level.SEVERE, null, ex);
                } catch (Exception ex) {
                    Logger.getLogger(LogFilter.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }).start();
    }

    private ArrayList<Object[]> divideToColumnsForTable(ArrayList<String> log, String delimiter) {
        ArrayList<Object[]> dataTableList = new ArrayList();
        int actualLine = 0;

        Iterator iterator = log.iterator();

        while (iterator.hasNext()) {
            listener.onUpdate(++actualLine, "Generating data table...");
            String row = (String) iterator.next();
            String[] parts = row.split(delimiter);
            Object[] objects = new Object[16];

            for (int j = 0; j < parts.length; j++) {
                if (j < 15) {
                    objects[j] = parts[j];
                } else {
                    objects[14] = (String) objects[14] + parts[j];
                                    }
            }

            dataTableList.add(objects);
        }

        return dataTableList;
    }

    private String calculateUnixTime(String fromDate) throws ParseException {
        String d = fromDate.substring(1, fromDate.length());
        DateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("31"));
        Date date = dateFormat.parse(d);
        long diff = date.getTime();
        
        return TimeUnit.MILLISECONDS.toSeconds(diff) + "";
    }
    
    private void generateUnixTime(ArrayList<Object[]> data) throws ParseException{
        for (int i = 0; i < data.size(); i++){
            String date = (String) data.get(i)[dateColumnNumber];
            data.get(i)[15] = calculateUnixTime(date);
            
        }
    }

    private void filterExtensions(ArrayList<Object[]> data) {
        List<Integer> toRemove = new ArrayList<Integer>();
        for (int i = 0; i < data.size(); i++) {
            listener.onUpdate(i + 1, "Filtering file extensions...");
            String cell = (String) data.get(i)[URLColumnNumber];
            fileTypeLoop:
            for (String fileType : fileTypes) {
                if (cell.contains(fileType)) {
                    toRemove.add(i);
                    break fileTypeLoop;
                }
            }
        }
        Collections.reverse(toRemove);
        for (Integer position : toRemove) {
            data.remove(position.intValue());
        }
    }

    private void filterStatusCodesAndMethods(ArrayList<Object[]> data) {
        List<Integer> toRemove = new ArrayList<Integer>();
        for (int i = 0; i < data.size(); i++) {
            listener.onUpdate(i + 1, "Filtering status codes and methods...");
            String statusCode = (String) data.get(i)[statusCodeColumnNumber];
            String method = (String) data.get(i)[methodColumnNumber];
            if (!method.contains("GET") | (!statusCode.startsWith("2") && !statusCode.startsWith("3"))) {
                toRemove.add(i);
            }
        }
        Collections.reverse(toRemove);
        for (Integer position : toRemove) {
            data.remove(position.intValue());
        }
    }

    private void filterRobots(ArrayList<Object[]> data) {
        ArrayList<String> robotsIP = new ArrayList();
        for (int i = 0; i < data.size(); i++) {
            listener.onUpdate(i + 1, "Finding robots...");
            String url = (String) data.get(i)[URLColumnNumber];
            if (url.contains("robots.txt")) {
                robotsIP.add((String) data.get(i)[0]);
            }
        }

        List<Integer> toRemove = new ArrayList<Integer>();
        for (int i = 0; i < data.size(); i++) {
            listener.onUpdate(i + 1, "Filtering robots...");
            String ipAddress = (String) data.get(i)[0];
            String agent = (String) data.get(i)[agentColumnNumber] + " ";
            if (robotsIP.contains(ipAddress) || agent.contains("bot") || agent.contains("crawler")) {
                toRemove.add(i);
            }
        }

        Collections.reverse(toRemove);
        for (Integer position : toRemove) {
            data.remove(position.intValue());
        }
    }

}
