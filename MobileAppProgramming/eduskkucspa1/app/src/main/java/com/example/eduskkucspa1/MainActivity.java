package com.example.eduskkucspa1;

import androidx.appcompat.app.AppCompatActivity;
import androidx.core.content.ContextCompat;

import android.content.Context;
import android.os.Bundle;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ListView;
import android.widget.TextView;
import android.widget.Toast;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class MainActivity extends AppCompatActivity {

    Button btn;
    EditText editText;
    ListView tryView;
    ListView bottomView1,bottomView2,bottomView3;
    String guess, answer;
    Character[] guessChar, answerChar;
    ArrayList<String> dict_words = new ArrayList<>();
    HashMap<String, Integer> mem = new HashMap<>();
    ArrayList<Character[]> wordList = new ArrayList<>();
    ArrayList<Integer[]> match = new ArrayList<>();

    ArrayAdapter bottomAdapter1;
    ArrayAdapter bottomAdapter2;
    ArrayAdapter bottomAdapter3;
    ArrayAdapter rawAdapter;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        btn=findViewById(R.id.button);
        editText=findViewById(R.id.editText);
        tryView = findViewById(R.id.tryView);
        bottomView1 = findViewById(R.id.bottomView1);
        bottomView2 = findViewById(R.id.bottomView2);
        bottomView3 = findViewById(R.id.bottomView3);
        for (char ch = 'A'; ch <= 'Z'; ++ch) mem.put(String.valueOf(ch), 3);

        try {
            InputStream is = getApplicationContext().getAssets().open("wordle_words.txt");
            BufferedReader in = new BufferedReader(new InputStreamReader(is));
            while(true){
                String search = in.readLine();
                if(search==null) break;
                dict_words.add(search);
            }
            in.close();
            is.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        setAnswer();

        Log.d("answer",Arrays.toString(answerChar));
    }
    public void setAnswer(){
        Random rand = new Random();
        answer = dict_words.get(rand.nextInt(dict_words.size()));
        answerChar=wordToChar(answer);
    }
    public void updateTop(){
        wordList.add(guessChar);
        matchWithAnswer();

        rawAdapter= new ArrayAdapter(this, R.layout.word_view, wordList){
            @Override
            public View getView(int row, View convertView, ViewGroup parent){
                LayoutInflater layoutInflater = (LayoutInflater) getSystemService(Context.LAYOUT_INFLATER_SERVICE);
                View currentView = layoutInflater.inflate(R.layout.word_view,parent,false);
                TextView textView1 = currentView.findViewById(R.id.textView1);
                TextView textView2 = currentView.findViewById(R.id.textView2);
                TextView textView3 = currentView.findViewById(R.id.textView3);
                TextView textView4 = currentView.findViewById(R.id.textView4);
                TextView textView5 = currentView.findViewById(R.id.textView5);
                TextView[] viewArr = new TextView[] {textView1,textView2,textView3,textView4,textView5};
                for (int i=0;i<5;i++){
                    TextView textView = viewArr[i];
                    textView.setText(wordList.get(row)[i].toString());
                    if(match.get(row)[i]==0){
                        textView.setTextColor(ContextCompat.getColor(getApplicationContext(), R.color.black));
                        textView.setBackgroundColor(ContextCompat.getColor(getApplicationContext(), R.color.green));
                    }
                    else if(match.get(row)[i]==1){
                        textView.setTextColor(ContextCompat.getColor(getApplicationContext(), R.color.black));
                        textView.setBackgroundColor(ContextCompat.getColor(getApplicationContext(), R.color.yellow));
                    }
                    else{
                        textView.setTextColor(ContextCompat.getColor(getApplicationContext(), R.color.white));
                        textView.setBackgroundColor(ContextCompat.getColor(getApplicationContext(), R.color.gray));
                    }
                }
                return currentView;
            }
        };
        tryView.setAdapter(rawAdapter);
        if(guess.equals(answer)){
            String msg="Congratulation!";
            Toast.makeText(getApplicationContext(),msg,Toast.LENGTH_SHORT).show();
        }
        updateBottom();
    }
    public void matchWithAnswer(){
        Integer[] current = new Integer[5];
        for(int i=0;i<5;i++){
            boolean noMatch=true;
            if (guessChar[i]==answerChar[i]){
                current[i]=0;
                mem.put(Character.toString(guessChar[i]),0);
                noMatch=false;
            }else{
                for(int j=0;j<5;j++){
                    if(guessChar[i]==answerChar[j]){
                        current[i]=1;
                        if(mem.get(Character.toString(guessChar[i]))!=0){
                            mem.put(Character.toString(guessChar[i]),1);
                            noMatch=false;
                            break;
                        }
                    }
                }
                if(noMatch){
                    current[i]=2;
                    if(mem.get(Character.toString(guessChar[i]))>2) mem.put(Character.toString(guessChar[i]),2);
                }
            }
        }
        match.add(current);
    }
    public void searchDict(View view) {
        guess = editText.getText().toString();
        boolean found=false;
        for(String search:dict_words){
            if(guess.equals(search)){
                found=true;
                break;
            }
        }
        if(found){
            guessChar=wordToChar(guess);
            updateTop();
        }
        else{
            String msg="Not in dictionary. Try again.";
            Toast.makeText(getApplicationContext(),msg,Toast.LENGTH_SHORT).show();
        }
        editText.setText("");
    }
    public Character[] wordToChar(String word){
        Character[] ch= new Character[5];
        ch[0]=Character.toUpperCase(word.charAt(0));
        ch[1]=Character.toUpperCase(word.charAt(1));
        ch[2]=Character.toUpperCase(word.charAt(2));
        ch[3]=Character.toUpperCase(word.charAt(3));
        ch[4]=Character.toUpperCase(word.charAt(4));
        return ch;
    }
    public ArrayList<String> categorize(int color){
        ArrayList<String> items = new ArrayList<>();
        for(String ch:mem.keySet()){
            if(mem.get(ch)==color){
                items.add(ch);
            }
        }
        return items;
    }
    public void updateBottom(){
        bottomAdapter1= new ArrayAdapter(this, R.layout.letter_view, categorize(2)){
            @Override
            public View getView(int position, View convertView, ViewGroup parent){
                LayoutInflater layoutInflater = (LayoutInflater) getSystemService(Context.LAYOUT_INFLATER_SERVICE);
                View view = layoutInflater.inflate(R.layout.letter_view,parent,false);
                TextView letter = view.findViewById(R.id.letter);
                letter.setText(categorize(2).get(position));
                letter.setTextColor(ContextCompat.getColor(getApplicationContext(), R.color.white));
                letter.setBackgroundColor(ContextCompat.getColor(getApplicationContext(), R.color.gray));
                return view;
            }
        };
        bottomAdapter2= new ArrayAdapter(this, R.layout.letter_view, categorize(1)){
            @Override
            public View getView(int position, View convertView, ViewGroup parent){
                LayoutInflater layoutInflater = (LayoutInflater) getSystemService(Context.LAYOUT_INFLATER_SERVICE);
                View view = layoutInflater.inflate(R.layout.letter_view,parent,false);
                TextView letter = view.findViewById(R.id.letter);
                letter.setText(categorize(1).get(position));
                letter.setTextColor(ContextCompat.getColor(getApplicationContext(), R.color.black));
                letter.setBackgroundColor(ContextCompat.getColor(getApplicationContext(), R.color.yellow));
                return view;
            }
        };
        bottomAdapter3= new ArrayAdapter(this, R.layout.letter_view, categorize(0)){
            @Override
            public View getView(int position, View convertView, ViewGroup parent){
                LayoutInflater layoutInflater = (LayoutInflater) getSystemService(Context.LAYOUT_INFLATER_SERVICE);
                View view = layoutInflater.inflate(R.layout.letter_view,parent,false);
                TextView letter = view.findViewById(R.id.letter);
                letter.setText(categorize(0).get(position));
                letter.setTextColor(ContextCompat.getColor(getApplicationContext(), R.color.black));
                letter.setBackgroundColor(ContextCompat.getColor(getApplicationContext(), R.color.green));
                return view;
            }
        };
        bottomView1.setAdapter(bottomAdapter1);
        bottomView2.setAdapter(bottomAdapter2);
        bottomView3.setAdapter(bottomAdapter3);
    }
}