package edu.skku.cs.pa2;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;

import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.Button;
import android.widget.ListView;
import android.widget.TextView;
import android.widget.Toast;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class MapSelectionActivity extends AppCompatActivity {
    TextView textView;
    MazeViewAdapter mazeViewAdapter;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_map_selection);

        Intent intent = getIntent();
        String name = intent.getStringExtra("USERNAME");
        ListView listView = findViewById(R.id.listView);
        textView = findViewById(R.id.textView2);
        textView.setText(name);

        OkHttpClient client = new OkHttpClient();
        HttpUrl.Builder urlBuilder = HttpUrl.parse("http://115.145.175.57:10099/maps").newBuilder();
        String url = urlBuilder.build().toString();
        Request req = new Request.Builder().url(url).build();

        client.newCall(req).enqueue(new Callback() {
            @Override
            public void onFailure(@NonNull Call call, @NonNull IOException e) {
                e.printStackTrace();
            }

            @Override
            public void onResponse(@NonNull Call call, @NonNull Response response) throws IOException {
                final String myResponse = response.body().string();
                Gson gson = new GsonBuilder().create();
                final List<Map> list = gson.fromJson(myResponse, List.class);


                MapSelectionActivity.this.runOnUiThread(new Runnable() {

                    @Override
                    public void run() {
                        mazeViewAdapter = new MazeViewAdapter(list, getApplicationContext());
                        listView.setAdapter(mazeViewAdapter);

                    }
                });
            }
        });
    }

    class MazeViewAdapter extends BaseAdapter{
        List<Map> list;
        Context mContext;
        MazeViewAdapter(List<Map> list, Context mContext){
            this.list = list;
            this.mContext = mContext;
        }

        @Override
        public int getCount() {
            return list.size();
        }

        @Override
        public Object getItem(int i) {
            return list.get(i);
        }

        @Override
        public long getItemId(int i) {
            return i;
        }

        @Override
        public View getView(int i, View view, ViewGroup viewGroup) {
            if(view == null){
                LayoutInflater layoutInflater = (LayoutInflater)  mContext.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
                view = layoutInflater.inflate(R.layout.maze_view,viewGroup, false);
            }
            String name = list.get(i).get("name").toString();
            String size = list.get(i).get("size").toString();

            TextView nameView = view.findViewById(R.id.nameView);
            TextView sizeView = view.findViewById(R.id.sizeView);
            nameView.setText(name);
            sizeView.setText(size);
            Button button = view.findViewById(R.id.button2);
            button.setOnClickListener(view1 -> {

                OkHttpClient client = new OkHttpClient();
                HttpUrl.Builder urlBuilder = HttpUrl.parse("http://115.145.175.57:10099/maze/map").newBuilder();
                urlBuilder.addQueryParameter("name",name);
                String url = urlBuilder.build().toString();
                Request req = new Request.Builder().url(url).build();

                client.newCall(req).enqueue(new Callback() {
                    @Override
                    public void onFailure(@NonNull Call call, @NonNull IOException e) {
                        e.printStackTrace();
                    }

                    @Override
                    public void onResponse(@NonNull Call call, @NonNull Response response) throws IOException {
                        final String myResponse = response.body().string();
                        Gson gson = new GsonBuilder().create();

                        final MazeString mazeString = gson.fromJson(myResponse, MazeString.class);

                        MapSelectionActivity.this.runOnUiThread(new Runnable() {

                            @Override
                            public void run() {
                                Intent intent = new Intent(MapSelectionActivity.this, MazeActivity.class);
                                intent.putExtra("MAZE",mazeString.maze);
                                startActivity(intent);
                            }
                        });
                    }
                });
            });

            return view;
        }
    }
    class MazeString{
        String maze;
    }
}