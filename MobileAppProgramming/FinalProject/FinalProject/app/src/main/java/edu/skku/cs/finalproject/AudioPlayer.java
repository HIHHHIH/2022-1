package edu.skku.cs.finalproject;

import static java.lang.Thread.sleep;

import android.media.AudioManager;
import android.media.MediaPlayer;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.os.Message;

import androidx.annotation.NonNull;

import java.io.IOException;
import java.lang.ref.WeakReference;


public class AudioPlayer extends MediaPlayer{
    static int duration;
    ProgressHandler progressHandler;
    Thread timeChecker;

    public AudioPlayer(){
        progressHandler = new ProgressHandler(this);

    }

    public void setAudioTrack(String url){
        setAudioStreamType(AudioManager.STREAM_MUSIC);
        try {
            setDataSource(url);
            prepare();
        } catch (IOException e) {
            e.printStackTrace();
        }
        duration= this.getDuration();
    }

    public void startPlaying(){
        this.start();
        startTimeChecker(this);
    }

    void startTimeChecker(AudioPlayer player){
        timeChecker = new Thread(new Runnable() {
            @Override
            public void run() {
                while(player.isPlaying()){
                    int pos = player.getCurrentPosition();
                    Bundle bundle = new Bundle();
                    bundle.putInt("pos",pos);
                    Message msg = new Message();
                    msg.setData(bundle);
                    progressHandler.sendMessage(msg);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        timeChecker.start();
    }

    public void stopPlaying(){
        this.pause();
    }

    public static class ProgressHandler extends Handler {
        //private final WeakReference<AudioPlayer> weakReference;

        public ProgressHandler(AudioPlayer audioPlayer){
            //this.weakReference = new WeakReference<>(audioPlayer);
        }

        @Override
        public void handleMessage(Message msg){
            super.handleMessage(msg);
            Bundle bundle = msg.getData();
            int pos = bundle.getInt("pos");
            ViewModel2.progress.postValue(pos);
        }
    }



}
