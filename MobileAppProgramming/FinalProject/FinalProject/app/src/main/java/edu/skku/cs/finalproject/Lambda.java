package edu.skku.cs.finalproject;

import android.content.Context;
import android.util.Log;

import androidx.annotation.NonNull;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.util.ArrayList;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class Lambda {
    final static String SEARCHING_FILE = "searching audio file...";
    final static String AUDIO_FILE_FOUND = "audio file found!";
    final static String AUDIO_FILE_NOT_FOUND = "we don't have that song on the S3";
    final static String ASKINGURL = "asking file url";
    final static String DOWNLOADING = "downloading audio file";
    final static String COMPLETED = "downloaded completed";

    //String homeUrl = "http://10.0.2.2:8888";
    static String homeUrl = "https://vzh2kpy25l.execute-api.ap-northeast-2.amazonaws.com/dev";
    static String preSignedUrl;
    static String fileName;
    static ArrayList<String> songData = new ArrayList<>();
    Context context;
    History history;
    public Lambda(Context context,History history){
        this.context = context;
        this.history = history;
    }



    public static void askFileName(String title, String artist, String subject, String coverArtUrl) {
        //setStatus(SEARCHING_FILE);

        HttpUrl.Builder urlBuilder = HttpUrl.parse(homeUrl).newBuilder();
        urlBuilder.addPathSegment("search_song");
        urlBuilder.addQueryParameter("title", title);
        urlBuilder.addQueryParameter("artist", artist);
        String url = urlBuilder.build().toString();
        Log.d("tag search_song url",url);
        Request req = new Request.Builder().url(url).build();

        OkHttpClient client = new OkHttpClient();
        client.newCall(req).enqueue(new Callback() {
            @Override
            public void onFailure(@NonNull Call call, @NonNull IOException e) {
                e.printStackTrace();
            }

            @Override
            public void onResponse(@NonNull Call call, @NonNull Response response) throws IOException {
                final String myResponse = response.body().string();
                Gson gson = new GsonBuilder().create();
                SearchResponse searchResponse = gson.fromJson(myResponse, SearchResponse.class);

                boolean success = Boolean.parseBoolean(searchResponse.success);
                fileName = searchResponse.file_name;
                if(success){
                    Log.d("tag", "file found: " + searchResponse.file_name);
                    //ViewModel2.setStatus(AUDIO_FILE_FOUND);
                    songData.clear();
                    songData.add(title);
                    songData.add(artist);
                    songData.add(subject);
                    songData.add(coverArtUrl);
                }else{
                    Log.d("tag", "search failed");
                    //setStatus(AUDIO_FILE_NOT_FOUND);
                }

                getPreSignedUrl(fileName);
            }
        });
    }

    public static void getPreSignedUrl(String fileName){
        //setStatus(ASKINGURL);

        HttpUrl.Builder urlBuilder = HttpUrl.parse(homeUrl).newBuilder();
        urlBuilder.addPathSegment("get_url");
        urlBuilder.addQueryParameter("file_name", fileName);
        String url = urlBuilder.build().toString();
        Log.d("tag ask pre-signed url",url);
        Request req = new Request.Builder().url(url).build();

        OkHttpClient client = new OkHttpClient();
        client.newCall(req).enqueue(new Callback() {
            @Override
            public void onFailure(@NonNull Call call, @NonNull IOException e) {
                e.printStackTrace();
            }

            @Override
            public void onResponse(@NonNull Call call, @NonNull Response response) throws IOException {
                preSignedUrl = response.body().string();
                Log.d("tag pre-signed url",preSignedUrl);
                History.tempUrl = preSignedUrl;
                History.saveLog(songData);


                ViewModel2.audioReady.postValue(true);
            }
        });
    }

    static class SearchResponse{
        String file_name;
        String success;
    }
}
