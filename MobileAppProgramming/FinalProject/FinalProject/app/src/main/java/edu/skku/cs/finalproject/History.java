package edu.skku.cs.finalproject;

import android.content.Context;
import android.util.Log;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class History {
    public static final String logFileName = "song_log.txt";
    private static String cachePath;
    private static String root ;
    private static String logPath;
    public static String tempShazam = new String();
    public static String tempUrl = new String();
    private static final String delimiter = "DELIMITER";

    public static String getCachePath() {
        return cachePath;
    }
    public static void setCachePath(String cachePath) {
        History.cachePath = cachePath;
    }
    public static String getRoot() {
        return root;
    }
    public static void setRoot(String root) {
        History.root = root;
    }
    public static String getLogPath() {
        return logPath;
    }
    public static void setLogPath(String logPath) {
        History.logPath = logPath;
    }

    Context context;
    public History(Context context){
        this.context = context;
        root = context.getFilesDir().getAbsolutePath();
        logPath = root + "/" +logFileName;
    }


    public static void saveLog(ArrayList<String> songData){
        boolean exist = searchLog(songData.get(0));
        if(!exist){
            try {
                FileWriter fw = new FileWriter(logPath,true);
                BufferedWriter bw = new BufferedWriter(fw);
                String line = songData.get(0) + delimiter + songData.get(1) + delimiter + songData.get(2) + delimiter + songData.get(3);
                bw.write(line);
                bw.newLine();
                bw.close();
                fw.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean searchLog(String subject){
        try {
            BufferedReader br = new BufferedReader(new FileReader(logPath));
            while(true){
                String line = br.readLine();
                if(line == null) {
                    break;
                }
                String[] log = line.split(delimiter);
                Log.d("tag logsubject", log[0]);
                if(subject.equals(log[0])){
                    Log.d("tag is log exist", "true");
                    return true;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void readLog(){
        try {
            BufferedReader br = new BufferedReader(new FileReader(logPath));
            int count=0;
            while(true){
                String line = br.readLine();
                if(line == null) break;
                Log.d("tag log", String.valueOf(count) + " " + line);
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ArrayList<String[]> getSongList(){
        ArrayList<String[]> songList = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(logPath));
            int count = 0;
            while(true){
                String line = br.readLine();
                if(line == null) break;
                Log.d("tag Line",line);
                String[] item = line.split(delimiter);
                songList.add(item);
                Log.d("tag song", String.valueOf(count)+ " " + item[0]);
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return songList;
    }

}
