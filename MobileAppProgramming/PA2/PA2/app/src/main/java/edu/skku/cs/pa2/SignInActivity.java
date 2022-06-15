package edu.skku.cs.pa2;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class SignInActivity extends AppCompatActivity {
    Button btn;
    TextView textView;
    EditText editText;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_sign_in);

        textView = findViewById(R.id.textView);
        btn = findViewById(R.id.button);
        editText = findViewById(R.id.editTextTextPersonName);

        btn.setOnClickListener(view -> {
            OkHttpClient client = new OkHttpClient();

            HttpUrl.Builder urlBuilder = HttpUrl.parse("http://115.145.175.57:10099/users").newBuilder();

            String username = editText.getText().toString();
            JSONObject json = new JSONObject();
            try {
                json.put("username",username);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            String url = urlBuilder.build().toString();

            Request req = new Request.Builder().url(url).post(RequestBody.create(MediaType.parse("application/json; charset=utf-8"),json.toString())).build();

            client.newCall(req).enqueue(new Callback() {
                @Override
                public void onFailure(@NonNull Call call, @NonNull IOException e) {
                    e.printStackTrace();
                }

                @Override
                public void onResponse(@NonNull Call call, @NonNull Response response) throws IOException {
                    final String myResponse = response.body().string();
                    Gson gson = new GsonBuilder().create();
                    final SignInResponse signInResponse = gson.fromJson(myResponse, SignInResponse.class);

                    SignInActivity.this.runOnUiThread(new Runnable() {

                        @Override
                        public void run() {
                            String text = signInResponse.success;
                            if(text.equals("true")){
                                Intent intent = new Intent(SignInActivity.this, MapSelectionActivity.class);
                                intent.putExtra("USERNAME",username);
                                startActivity(intent);
                            }else{
                                Toast.makeText(getApplicationContext(), "Wrong User Name", Toast.LENGTH_SHORT).show();
                            }
                        }
                    });
                }
            });
        });
    }

    class SignInResponse{
        String success;
    }
}