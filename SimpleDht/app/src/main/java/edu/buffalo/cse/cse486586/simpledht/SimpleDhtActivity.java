package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.widget.TextView;
import android.util.Log;

public class SimpleDhtActivity extends Activity {
    static ContentResolver mContentResolver;
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    private Uri buildUri2(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    Uri mUri = buildUri2("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        Log.d(TAG, "onCreate activity");
//
//        try {
//            String result =  mContentResolver.getType(mUri);
//            //System.out.println("Triggered the on create method "+ result);
//        }
//        catch (Exception e){
//            e.printStackTrace();
//        }



        
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
