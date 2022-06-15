package edu.skku.cs.finalproject;


import androidx.appcompat.app.AppCompatActivity;
import androidx.databinding.DataBindingUtil;
import androidx.lifecycle.Observer;

import android.content.Context;
import android.content.Intent;
import android.media.MediaPlayer;
import android.os.Bundle;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.SeekBar;
import android.widget.TextView;

import com.bumptech.glide.Glide;

import java.io.IOException;
import java.util.ArrayList;

import edu.skku.cs.finalproject.databinding.ActivityPlayerBinding;

public class PlayerActivity extends AppCompatActivity {

    ActivityPlayerBinding binding;
    ViewModel2 viewModel;

    String title;
    String artist;
    String coverArtUrl;
    String subject;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        Intent intent = getIntent();
        title = intent.getStringExtra("title");
        artist = intent.getStringExtra("artist");
        coverArtUrl = intent.getStringExtra("coverArt");
        subject=  intent.getStringExtra("subject");


        binding = DataBindingUtil.setContentView(this, R.layout.activity_player);
        TextView titleView = binding.titleView;
        TextView artistView = binding.artistView;
        //ImageView imageView = binding.imageView;
        titleView.setText(title);
        artistView.setText(artist);
        //Glide.with(this).load(coverArtUrl).override(400,400).into(imageView);
        binding.setImageUrl(coverArtUrl);
        binding.setViewModel2(new ViewModel2(PlayerActivity.this));

        binding.button2.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Intent intent1 = new Intent(PlayerActivity.this,Library.class);
                startActivity(intent1);
            }
        });

        SeekBar seekBar = binding.seekBar;
        seekBar.setOnSeekBarChangeListener(new SeekBar.OnSeekBarChangeListener() {
            @Override
            public void onProgressChanged(SeekBar seekBar, int progress, boolean fromUser) {
                if(fromUser) viewModel.getAudioPlayer().seekTo(progress);
            }

            @Override
            public void onStartTrackingTouch(SeekBar seekBar) {

            }

            @Override
            public void onStopTrackingTouch(SeekBar seekBar) {

            }
        });

        final Observer<Boolean> audioObserver = new Observer<Boolean>() {
            @Override
            public void onChanged(Boolean aBoolean) {
                if (aBoolean){
                    viewModel.getAudioPlayer().setAudioTrack(Lambda.preSignedUrl);
                    seekBar.setMax(AudioPlayer.duration);
                }
            }
        };
        final Observer<Integer> progressObserver = new Observer<Integer>() {
            @Override
            public void onChanged(Integer integer) {
                seekBar.setProgress(integer);
            }
        };
        viewModel = binding.getViewModel2();
        viewModel.getAudioReady().observe(this, audioObserver);
        viewModel.getProgress().observe(this, progressObserver);
        History history = new History(getApplicationContext());
        Lambda lambda = new Lambda(getApplicationContext(), history);
        lambda.askFileName(title,artist, subject, coverArtUrl);
    }


}