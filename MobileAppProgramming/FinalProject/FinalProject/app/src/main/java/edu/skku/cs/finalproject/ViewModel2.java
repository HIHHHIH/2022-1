package edu.skku.cs.finalproject;

import android.content.Context;
import android.util.Log;
import android.widget.ImageView;

import androidx.annotation.NonNull;
import androidx.databinding.BaseObservable;
import androidx.databinding.Bindable;
import androidx.databinding.BindingAdapter;
import androidx.lifecycle.MutableLiveData;

import com.bumptech.glide.Glide;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class ViewModel2 extends BaseObservable {


    AudioPlayer player;

    String playArrowUrl = "https://cdn4.iconfinder.com/data/icons/ionicons/512/icon-play-1024.png";
    String pauseUrl ="https://cdn0.iconfinder.com/data/icons/typicons-2/24/media-pause-1024.png";

    public static MutableLiveData<Integer> progress = new MutableLiveData<>();
    public static MutableLiveData<Boolean> audioReady = new MutableLiveData<>();

    public ViewModel2(Context context){
        player = new AudioPlayer();
    }

    public MutableLiveData<Integer> getProgress() {
        if(progress == null){
            progress = new MutableLiveData<>();
        }
        return progress;
    }

    public MutableLiveData<Boolean> getAudioReady() {
        if(audioReady == null){
            audioReady = new MutableLiveData<>();
        }
        return audioReady;
    }


    boolean mStartPlaying = false;
    @Bindable
    public String status = " ";

    @Bindable
    public String playIconUrl = playArrowUrl;

    public String getPlayIconUrl() {
        return playIconUrl;
    }
    public void setPlayIconUrl(String playIconUrl) {
        this.playIconUrl = playIconUrl;
        notifyPropertyChanged(BR.playIconUrl);

    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        notifyPropertyChanged(BR.status);
    }


    public void onPlay() {
        mStartPlaying = !mStartPlaying;
        if(mStartPlaying){
            player.startPlaying();
            setPlayIconUrl(pauseUrl);
        }
        else{
            player.stopPlaying();
            setPlayIconUrl(playArrowUrl);
        }
    }

    @BindingAdapter("changeImage")
    public static void changeImage(ImageView view, String imageUrl){
        Glide.with(view.getContext()).load(imageUrl).override(100,100).into(view);
    }

    @BindingAdapter("coverImage")
    public static void loadImage(ImageView view, String imageUrl){
        Glide.with(view.getContext()).load(imageUrl).override(400,400).into(view);
    }

    public AudioPlayer getAudioPlayer(){
        return player;
    }


}
