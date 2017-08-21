package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

import java.util.List;
import java.util.Map;

public class SimpleDynamoActivity extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {

        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dynamo);

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        registerListeners(tv);

        init();

    }


    private void init() {

        // set the content resolver
        GlobalRepo.setContentResolver(this.getContentResolver());

        buildURI();

        //initDb();
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.simple_dynamo, menu);
        return true;
    }

    public void onStop() {
        super.onStop();
        Log.v("Test", "onStop()");
    }


    /**
     *
     */
    private void buildURI() {

        Uri.Builder builder = new Uri.Builder();
        builder.scheme(Constants.CONTENT);
        builder.authority(Constants.AUTHORITY);
        Uri uri = builder.build();

        GlobalRepo.setUri(uri);
    }

    private void registerListeners(TextView tv) {

        findViewById(R.id.button1).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        findViewById(R.id.button2).setOnClickListener(

                new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {

                        TextView tv = (TextView) findViewById(R.id.textView1);
                        tv.setMovementMethod(new ScrollingMovementMethod());
                        tv.append("");

                        List<String> ring = GlobalRepo.ring;
                        Map<String, String> reverseLookup = GlobalRepo.reverseLookup;

                        for (String node : ring) {
                            tv.append(reverseLookup.get(node) + " --> ");
                        }
                    }
                });

        findViewById(R.id.button3).setOnClickListener(

                new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {

                        TextView tv = (TextView) findViewById(R.id.textView1);
                        tv.setMovementMethod(new ScrollingMovementMethod());
                        tv.append("");
                        String[] successors = GlobalRepo.successors;
                        for (String succ : successors) {
                            tv.append(succ + ", ");
                        }
                        tv.append("\nFailed Node : " + SimpleDynamoProvider.failedNode);
                    }
                });
    }

}
