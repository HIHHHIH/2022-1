package edu.skku.cs.finalproject;

import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.RecyclerView;

import android.content.Context;
import android.content.Intent;
import android.media.AudioManager;
import android.media.MediaPlayer;
import android.net.Uri;
import android.os.Bundle;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.BaseAdapter;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.ListView;
import android.widget.TextView;
import android.widget.Toast;

import com.bumptech.glide.Glide;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.util.ArrayList;

public class Library extends AppCompatActivity {

    ListView listView;
    ArrayList<String[]> songInfoList;
    AudioPlayer player;
    boolean mStartPlaying = false;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_library);

        songInfoList = History.getSongList();

        ArrayList<String> songList = getSongSubject(songInfoList);
        SongListViewAdapter songListViewAdapter = new SongListViewAdapter(songInfoList,this);
        listView = findViewById(R.id.listView);
        listView.setAdapter(songListViewAdapter);

    }
    ArrayList<String> getSongSubject(ArrayList<String[]> songInfo){
        ArrayList<String> songList = new ArrayList<>();
        for(String[] song : songInfo){
            songList.add(song[2]);
        }
        return songList;
    }


    class SongListViewAdapter extends BaseAdapter{
        ArrayList<String[]> songInfoList;
        Context context;
        SongListViewAdapter(ArrayList<String[]> songInfoList, Context context){
            this.songInfoList = songInfoList;
            this.context = context;
        }
        @Override
        public int getCount() {
            return songInfoList.size();
        }

        @Override
        public Object getItem(int i) {
            return songInfoList.get(i);
        }

        @Override
        public long getItemId(int i) {
            return i;
        }

        @Override
        public View getView(int i, View view, ViewGroup viewGroup) {
            Log.d("tag item number", String.valueOf(i));
            LayoutInflater layoutInflater = (LayoutInflater) context.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
            view = layoutInflater.inflate(R.layout.song_item, viewGroup, false);

            String title = songInfoList.get(i)[0];
            String artist = songInfoList.get(i)[1];
            String subject = songInfoList.get(i)[2];
            String coverArtUrl = songInfoList.get(i)[3];

            TextView numberView = view.findViewById(R.id.numberView);
            TextView songNameView = view.findViewById(R.id.songNameView);
            ImageView imageView = view.findViewById(R.id.coverImageView);


            numberView.setText(String.valueOf(i+1));
            songNameView.setText(subject);
            songNameView.setOnClickListener(view1 -> {

                Intent intent = new Intent(Library.this,PlayerActivity.class);
                intent.putExtra("title", title);
                intent.putExtra("artist", artist);
                intent.putExtra("subject", subject);
                intent.putExtra("coverArt", coverArtUrl);
                startActivity(intent);
                Toast.makeText(context,"You selected " + subject,Toast.LENGTH_SHORT).show();
            });
            Glide.with(context).load(coverArtUrl).into(imageView);


            return view;
        }
    }

}