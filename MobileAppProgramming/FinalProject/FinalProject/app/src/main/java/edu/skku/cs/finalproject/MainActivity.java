package edu.skku.cs.finalproject;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.content.res.AppCompatResources;
import androidx.core.app.ActivityCompat;
import androidx.databinding.DataBindingUtil;
import androidx.lifecycle.Observer;

import android.Manifest;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.drawable.Drawable;
import android.media.MediaPlayer;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Toast;

import edu.skku.cs.finalproject.databinding.ActivityMainBinding;

public class MainActivity extends AppCompatActivity {

    ActivityMainBinding binding;
    ViewModel viewModel;

    private static final String TAG = "tag";
    private static final int REQUEST_RECORD_AUDIO_PERMISSION = 200;

    // Requesting permission to RECORD_AUDIO
    private boolean permissionToRecordAccepted = false;
    private final String[] permissions = {Manifest.permission.RECORD_AUDIO};

    public static Drawable micIcon, micOffIcon, playArrowIcon, pauseIcon;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        History.setCachePath(getApplicationContext().getExternalCacheDir().getAbsolutePath());
        History.setRoot(getApplicationContext().getFilesDir().getAbsolutePath());
        History.setLogPath(History.getRoot()+"/"+History.logFileName);
/*
        micIcon = AppCompatResources.getDrawable(this,R.drawable.ic_baseline_mic_24);
        micOffIcon = AppCompatResources.getDrawable(this,R.drawable.ic_baseline_mic_off_24);
        playArrowIcon = AppCompatResources.getDrawable(this,R.drawable.ic_baseline_play_arrow_24);
        pauseIcon = AppCompatResources.getDrawable(this,R.drawable.ic_baseline_pause_24);

 */
        binding = DataBindingUtil.setContentView(this, R.layout.activity_main);
        binding.setViewModel(new ViewModel(MainActivity.this));
        viewModel = binding.getViewModel();
        final Observer<SongInfo> songObserver = new Observer<SongInfo>() {
            @Override
            public void onChanged(SongInfo songInfo) {
                Toast.makeText(getApplicationContext(),"Song detected!", Toast.LENGTH_SHORT).show();
                Intent intent = new Intent(MainActivity.this,PlayerActivity.class);
                intent.putExtra("title", songInfo.track.getTitle());
                intent.putExtra("artist", songInfo.track.getSubtitle());
                intent.putExtra("subject", songInfo.track.getShare().getSubject());
                intent.putExtra("coverArt", songInfo.track.getImages().getCoverart());
                intent.putExtra("genre", songInfo.track.getGenres().getPrimary());
                intent.putExtra("sections",songInfo.track.sections);


                startActivity(intent);
            }
        };
        viewModel.getDetectedSong().observe(this,songObserver);

        ActivityCompat.requestPermissions(this, permissions, REQUEST_RECORD_AUDIO_PERMISSION);
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        switch (requestCode) {
            case REQUEST_RECORD_AUDIO_PERMISSION:
                permissionToRecordAccepted = grantResults[0] == PackageManager.PERMISSION_GRANTED;
                break;
        }
        if (!permissionToRecordAccepted) {
            Log.d(TAG,"permission not accepted");
            finish();
        }

    }
}