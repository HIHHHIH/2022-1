package edu.skku.cs.pa2;

import static java.lang.Integer.getInteger;
import static java.lang.Integer.parseInt;
import static java.lang.Long.toBinaryString;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Context;
import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.Matrix;
import android.graphics.drawable.Drawable;
import android.os.Bundle;
import android.util.Log;
import android.util.TypedValue;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.Button;
import android.widget.FrameLayout;
import android.widget.GridView;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

import java.util.ArrayList;
import java.util.Arrays;

public class MazeActivity extends AppCompatActivity {

    TextView turnView;

    private static final String TAG = "tag";
    int[] pos = {0,0};
    int[] goalPos;
    int turn=0;
    int mazeSize;
    int direction;
    boolean hintUsed=false;
    ArrayList<int[]> mazeInfo;
    ArrayList<String> mazeCells;
    CellAdapter cellAdapter;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_maze);

        turnView = findViewById(R.id.turnView);
        Button hintBtn = findViewById(R.id.hintBtn);
        Button rightB = findViewById(R.id.rightBtn);
        Button leftB = findViewById(R.id.leftBtn);
        Button UpB = findViewById(R.id.upBtn);
        Button downB = findViewById(R.id.downBtn);
        GridView gridView = findViewById(R.id.gridView);

        Intent intent = getIntent();
        String mazeString = intent.getStringExtra("MAZE");
        String[] mazeLine =mazeString.split("\n");
        mazeSize = parseInt(mazeLine[0]);
        goalPos = new int[]{mazeSize-1,mazeSize-1};


        mazeCells = new ArrayList<>();
        for(int i=0;i<mazeSize;i++){
            String[] line = mazeLine[i+1].split(" ");
            for(int j=0; j<mazeSize;j++){
                int decimal = parseInt(line[j]);
                String binary = toBinaryString(decimal);
                while(binary.length()<4){
                    binary = "0"+binary;
                }
                mazeCells.add(binary);
            }
        }
        Log.d(TAG,Arrays.toString(mazeCells.toArray()));

        mazeInfo = new ArrayList<>();
        int[] info = {0,0};
        for(int i=0;i<mazeSize*mazeSize;i++){
            mazeInfo.add(info);
        }
        int[] initCell = {1,0};

        mazeInfo.set(0,initCell);
        Log.d(TAG,Arrays.deepToString(mazeInfo.toArray()));
        gridView.setNumColumns(mazeSize);

        cellAdapter= new CellAdapter(mazeInfo, getApplicationContext());
        gridView.setAdapter(cellAdapter);
    }
    public void moveL(View view) {
        int[] temp = {pos[0],pos[1]-1};
        if(isInBound(temp)){
            if(getCellBin(pos).charAt(1)!='1'){
                updateAdapter(pos,temp);
                pos=temp;
                direction = 1;
            }
        }
        if(Arrays.equals(pos,goalPos)){
            String msg = "Finished!";
            Toast.makeText(getApplicationContext(), msg, Toast.LENGTH_SHORT).show();
        }
    }
    public void moveR(View view) {
        int[] temp = {pos[0],pos[1]+1};
        if(isInBound(temp)){
            if(getCellBin(pos).charAt(3)!='1'){
                updateAdapter(pos,temp);
                pos=temp;
                direction =3;
            }
        }
        if(Arrays.equals(pos,goalPos)){
            String msg = "Finished!";
            Toast.makeText(getApplicationContext(), msg, Toast.LENGTH_SHORT).show();
        }
    }
    public void moveU(View view) {
        int[] temp = {pos[0]-1,pos[1]};
        if(isInBound(temp)){
            if(getCellBin(pos).charAt(0)!='1'){
                updateAdapter(pos,temp);
                pos=temp;
                direction = 0;
            }
        }
        if(Arrays.equals(pos,goalPos)){
            String msg = "Finished!";
            Toast.makeText(getApplicationContext(), msg, Toast.LENGTH_SHORT).show();
        }
    }
    public void moveD(View view) {
        int[] temp = {pos[0]+1,pos[1]};
        if(isInBound(temp)){
            if(getCellBin(pos).charAt(2)!='1'){
                updateAdapter(pos,temp);
                pos=temp;
                direction = 2;
            }
        }
        if(Arrays.equals(pos,goalPos)){
            String msg = "Finished!";
            Toast.makeText(getApplicationContext(), msg, Toast.LENGTH_SHORT).show();
        }
    }

    boolean isInBound(int[] pos){
        if((0<=pos[0]&&pos[0]<mazeSize)&&(0<=pos[1]&&pos[1]<mazeSize)){
            return true;
        }else return false;
    }

    String getCellBin(int[] pos){
        return mazeCells.get(pos[0]*mazeSize+pos[1]);
    }

    void updateAdapter(int[] pos, int[] temp){
        int[] gone = {0,0};
        int[] arrived = {1,0};
        mazeInfo.set(convertPosToIndex(pos),gone);
        mazeInfo.set(convertPosToIndex(temp),arrived);
        turn+=1;
        updateTurnView(turn);
        cellAdapter.notifyDataSetChanged();
    }
    void updateTurnView(int turn){
        String display = "Turn : "+String.valueOf(turn);
        Log.d(TAG,"turn: "+turn);
        turnView.setText(display);
    }

    int convertPosToIndex(int[] pos){
        return pos[0]*mazeSize+pos[1];
    }

    public void getHint(View view) {
        if(!hintUsed){
            int[] nextStep;
            nextStep = searchDirection(pos);
            int[] here = {0,1};
            mazeInfo.set(convertPosToIndex(nextStep),here);
            cellAdapter.notifyDataSetChanged();
            hintUsed=true;
        }
    }

    int[] searchDirection(int[] pos){
        boolean a=false,b=false,c=false,d=false;
        int[] up = {pos[0]-1,pos[1]};
        int[] down = {pos[0]+1,pos[1]};
        int[] left = {pos[0],pos[1]-1};
        int[] right = {pos[0],pos[1]+1};

        if(getCellBin(pos).charAt(0)=='0')
            a = visitPath(up,pos);
        if(getCellBin(pos).charAt(2)=='0')
            b = visitPath(down,pos);
        if(getCellBin(pos).charAt(1)=='0')
            c = visitPath(left,pos);
       if(getCellBin(pos).charAt(3)=='0')
            d = visitPath(right,pos);

        if(a) return up;
        else if(b) return down;
        else if(c) return left;
        else if(d) return right;
        else{
            Log.d(TAG,"something wrong");
            return null;
        }
    }

    boolean visitPath(int[] pos, int[] before){
        boolean a=false,b=false,c=false,d=false;

        int[] up = {pos[0]-1,pos[1]};
        int[] down = {pos[0]+1,pos[1]};
        int[] left = {pos[0],pos[1]-1};
        int[] right = {pos[0],pos[1]+1};

        String bin = getCellBin(pos);
        if(Arrays.equals(pos,goalPos)){
            Log.d(TAG,"found goal");
            return true;
        } else{
            if(!isBlocked(pos)){
                if((!Arrays.equals(up,before))&&(bin.charAt(0)=='0')){
                    a = visitPath(up,pos);
                }
                if((!Arrays.equals(left,before))&&(bin.charAt(1)=='0')){
                    b = visitPath(left,pos);
                }
                if((!Arrays.equals(down,before))&&(bin.charAt(2)=='0')){
                    c = visitPath(down,pos);
                }
                if((!Arrays.equals(right,before))&&(bin.charAt(3)=='0')){
                    d = visitPath(right,pos);
                }
            }else return false;
        }

        return a||b||c||d;
    }

    boolean isBlocked(int[] pos){
        int[] start = {0,0};
        if(!Arrays.equals(pos,start)){
            String bin = getCellBin(pos);
            if(bin.equals("1110")) return true;
            if(bin.equals("1101")) return true;
            if(bin.equals("1011")) return true;
            if(bin.equals("0111")) return true;
        }
        return false;
    }


    class CellAdapter extends BaseAdapter {
        ArrayList<int[]> mazeInfo;
        Context mContext;
        CellAdapter(ArrayList<int[]> mazeInfo, Context mContext){
            this.mazeInfo = mazeInfo;
            this.mContext = mContext;
        }

        @Override
        public int getCount() {
            return mazeInfo.size();
        }

        @Override
        public int[] getItem(int i) {
            return mazeInfo.get(i);
        }

        @Override
        public long getItemId(int i) {
            return i;
        }

        @Override
        public View getView(int i, View view, ViewGroup viewGroup) {
            if(view == null){
                LayoutInflater layoutInflater = (LayoutInflater)  mContext.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
                view = layoutInflater.inflate(R.layout.maze_cell,viewGroup, false);
            }
            int left=0, top=0, right=0, bottom=0;

            String binary = mazeCells.get(i);

            if(binary.charAt(0)=='1'){
                top = convertDpToPx(3);
            }
            if(binary.charAt(1)=='1'){
                left = convertDpToPx(3);
            }
            if(binary.charAt(2)=='1'){
                bottom = convertDpToPx(3);
            }
            if(binary.charAt(3)=='1'){
                right = convertDpToPx(3);
            }

            int num = (int)Math.sqrt(getCount());

            ImageView imageView = view.findViewById(R.id.imageView);

            setMargins(imageView,left, top, right, bottom);
            view.getLayoutParams().height=convertDpToPx(350/num);

            ImageView imageView2 = view.findViewById(R.id.imageView2);

            if(getItem(i)[0]==0&&getItem(i)[1]==0){
                imageView2.setImageDrawable(null);
            }
            if(getItem(i)[1]==1) {
                imageView2.setImageResource(R.drawable.hint);
            }
            if(i==getCount()-1){
                imageView2.setImageResource(R.drawable.goal);
            }
            if(getItem(i)[0]==1){
                int img = R.drawable.user;
                switch (direction){
                    case 0: imageView2.setImageResource(R.drawable.user); break;
                    case 1: imageView2.setImageBitmap(rotateImg(img,270)); break;
                    case 2: imageView2.setImageBitmap(rotateImg(img,180)); break;
                    case 3: imageView2.setImageBitmap(rotateImg(img,90));
                }
            }
            imageView2.getLayoutParams().height=convertDpToPx(30*5/num);
            imageView2.getLayoutParams().width=convertDpToPx(30*5/num);

            return view;
        }

        int convertDpToPx(int dp){
            int px = (int) TypedValue.applyDimension(
                    TypedValue.COMPLEX_UNIT_DIP,
                    dp,
                    getApplicationContext().getResources().getDisplayMetrics()
            );
            return px;
        }

        private void setMargins (View view, int left, int top, int right, int bottom) {
            if (view.getLayoutParams() instanceof ViewGroup.MarginLayoutParams) {
                ViewGroup.MarginLayoutParams p = (ViewGroup.MarginLayoutParams) view.getLayoutParams();
                p.setMargins(left, top, right, bottom);
                view.requestLayout();
            }
        }

        Bitmap rotateImg(int img,int degree){
            Bitmap src = BitmapFactory.decodeResource(getResources(),img);
            Matrix matrix = new Matrix();
            matrix.postRotate(degree);
            return Bitmap.createBitmap(src, 0, 0, src.getWidth(),src.getHeight(), matrix, true);
        }
    }
}