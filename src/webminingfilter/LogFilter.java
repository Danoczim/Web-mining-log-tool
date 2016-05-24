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
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Danóczi & Kušický
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
    private final ArrayList<String> unfilteredLog = new ArrayList<>();
    private ArrayList<Object[]> dataList;

    private final ArrayList<String> weekDates;
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
    
    private static DateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);
    public static boolean isValidDate(String dateString) {
        try {
            dateFormat.parse(dateString);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }
    
    public LogFilter(File file, String delimiter, boolean filterDates, String startDate, UIFilterListener filterListener) {
        weekDates = new ArrayList<>();
        this.file = file;
        this.delimiter = delimiter;
        this.filterDates = filterDates;
        if (this.filterDates == true) {
            weekDates.add(startDate);
            Calendar calendar = Calendar.getInstance(Locale.ENGLISH);
            try {
                calendar.setTime(dateFormat.parse(startDate));
            } catch (ParseException ex) {
                Logger.getLogger(LogFilter.class.getName()).log(Level.SEVERE, null, ex);
            }
            for (int i = 0; i < 6; i++) {
                calendar.add(Calendar.DATE, 1);
                weekDates.add(dateFormat.format(calendar.getTime()));
            }
        }
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

    public void setDateColumnNumber(int dateColumnNumber) {
        this.dateColumnNumber = dateColumnNumber - 1;
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
                if (agentColumnNumber == -1) {
                    listener.onFilterFileError("Provide agent column number!");
                    return;
                }
                if (dateColumnNumber == -1) {
                    listener.onFilterFileError("Provide date column number!");
                    return;
                }

                filterExtensions(dataList);

                listener.onFinish(dataList.size(), dataList);

                filterStatusCodesAndMethods(dataList);

                listener.onFinish(dataList.size(), dataList);

                filterRobots(dataList);

                generateUnixTime(dataList);

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
                            for (String day : weekDates) {
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
            Object[] objects = new Object[12 + 1]; // +1 for unix time
            for (int j = 0; j < parts.length; j++) {
                if (j < 12) {
                    objects[j] = parts[j];
                } else {
                    objects[11] = (String) objects[11] + parts[j];
                }
            }
            dataTableList.add(objects);
        }
        return dataTableList;
    }

    private void filterExtensions(ArrayList<Object[]> data) {
        List<Integer> toRemove = new ArrayList<>();
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
        List<Integer> toRemove = new ArrayList<>();
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

        List<Integer> toRemove = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            listener.onUpdate(i + 1, "Filtering robots...");
            String ipAddress = (String) data.get(i)[0];
            String agent = (String) data.get(i)[agentColumnNumber];
            if (robotsIP.contains(ipAddress) || agent.contains("bot") || agent.contains("crawler") || agent.contains("spider") || agent.contains("crawl")) {
                toRemove.add(i);
            }
        }

        Collections.reverse(toRemove);
        for (Integer position : toRemove) {
            data.remove(position.intValue());
        }
    }

    private void generateUnixTime(ArrayList<Object[]> data) {
        try {
            DateFormat localDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
            localDateFormat.setTimeZone(TimeZone.getTimeZone("Etc/GMT"));
            for (int i = 0; i < data.size(); i++) {
                listener.onUpdate(i + 1, "Generating unix time...");
                String dateString = (String) data.get(i)[dateColumnNumber];
                dateString = dateString.substring(1, dateString.length());
                Date date = localDateFormat.parse(dateString);
                data.get(i)[data.get(i).length - 1] = TimeUnit.MILLISECONDS.toSeconds(date.getTime());
            }
        } catch (ParseException ex) {
            Logger.getLogger(LogFilter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void saveDataToFile(File saveFile) {
        if (dataList == null) {
            listener.onFilterFileError("You have to load file first!");
            return;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < dataList.size(); i++) {
            listener.onUpdate(i + 1, "Saving to file...");
            for (Object row1 : dataList.get(i)) {
                sb.append(row1).append(" ");
            }
            sb.append("\n");
        }

        try (FileWriter fw = new FileWriter(saveFile + ".txt")) {
            fw.write(sb.toString());
        } catch (IOException e) {
            Logger.getLogger(LogFilter.class.getName()).log(Level.SEVERE, null, e);
        }
        listener.onUpdate(0, "Saving done");
    }
}