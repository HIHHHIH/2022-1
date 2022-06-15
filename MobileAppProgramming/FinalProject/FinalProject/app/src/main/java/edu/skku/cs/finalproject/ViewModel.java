package edu.skku.cs.finalproject;

import android.content.Context;
import android.media.AudioManager;

import androidx.databinding.BaseObservable;
import androidx.databinding.Bindable;
import androidx.lifecycle.MutableLiveData;

public class ViewModel extends BaseObservable {

    private final Media media;
    private final Shazam shazam;
    private MutableLiveData<SongInfo> detectedSong;

    String recordingFile;
    public static Boolean mStartRecording = false;
    public static Boolean mStartPlaying = false;
    AudioPlayer player;

    public ViewModel(Context context){
        recordingFile = context.getExternalCacheDir().getAbsolutePath() +"/record.pcm";
        media = new Media(recordingFile);
        shazam = new Shazam();

        player = new AudioPlayer();
    }

    public MutableLiveData<SongInfo> getDetectedSong() {
        if (detectedSong == null){
            detectedSong = new MutableLiveData<>();
        }
        return detectedSong;
    }

    @Bindable
    private String recordBtnText = "RECORD";
    @Bindable
    private String playBtnText = "PLAY";
    @Bindable
    private String song = null;

    public void setRecordBtnText(String recordBtnText) {
        this.recordBtnText = recordBtnText;
    }

    public void setPlayBtnText(String playBtnText) {
        this.playBtnText = playBtnText;
    }

    public void setSong(String song){
        this.song = song;
        notifyPropertyChanged(BR.song);
    }

    public String getRecordBtnText() {
        notifyPropertyChanged(BR.recordBtnText);
        return recordBtnText;
    }

    public String getPlayBtnText() {
        notifyPropertyChanged(BR.playBtnText);
        return playBtnText;
    }

    public String getSong(){
        return song;
    }

    public void onRecord() {
        mStartRecording = !mStartRecording;
        if (mStartRecording) {
            setRecordBtnText("STOP");
            media.startRecording();
        } else {
            setRecordBtnText("RECORD");
            media.stopRecording();
        }

    }

    public void onPlay(){
        mStartPlaying = !mStartPlaying;
        if (mStartPlaying) {
            setPlayBtnText("STOP");
            media.startPlaying(this);
        } else {
            setPlayBtnText("PLAY");
            media.stopPlaying();
        }
    }

    public void onShazam(){
        shazam.requestShazam(this,Media.base64String);
    }

}
