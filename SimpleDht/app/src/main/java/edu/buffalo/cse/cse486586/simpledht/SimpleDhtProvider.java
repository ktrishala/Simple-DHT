package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Comparator;
import java.util.*;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;
import android.widget.TextView;
import android.content.ContentValues;
import android.content.Context;
import android.content.ContentResolver;



public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String REMOTE_PORT[] = {"11108", "11112", "11116", "11120", "11124"};
    static List<nodeid> chordDHT = new ArrayList<nodeid>();
    static String myPort = null;
    static String activenodes = null;
    static int global_cnt =0;
    static int currindex=0;
    static int previndex =0;
    static int succindex=0;
    static int DHTsize =0;
    static final int SERVER_PORT = 10000;
    int avdno = 0;
    String myporthash = null;
    private Uri buildUri2(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    Uri mUri = buildUri2("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    /*
    This class is used for creating node object having the port number and hash code of the node
    Referenced from : https://www.geeksforgeeks.org/java-util-arrays-equals-java-examples/
     */
    public class nodeid {
        public String nodeport;
        public String nodehash;
        public nodeid(String nodeport, String nodehash) {
            this.nodeport = nodeport;
            this.nodehash = nodehash;
        }
        public String getport(){
            return nodeport;
        }
        public String getnodehash() {
            return nodehash;
        }
        public boolean equals(Object o){
            nodeid newobject =  (nodeid)(o);
            if((this.nodeport.equals(newobject.nodeport))&&(this.nodehash.equals(newobject.nodehash))){
                return  true;
            }
            return false;
        }
    }
    /*
    Comparator to sort the nodes based on their hash code
    Referenced from : https://www.geeksforgeeks.org/comparable-vs-comparator-in-java/
     */
    class sortbyhash implements Comparator<nodeid>
    {
        public int compare(nodeid a, nodeid b)
        {
            return a.getnodehash().compareTo(b.getnodehash());
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        File directory = getContext().getFilesDir();

        /*
        deleting in local
        Referenced from : https://www.geeksforgeeks.org/delete-file-using-java/
                          https://stackoverflow.com/questions/17729228/java-read-many-txt-files-on-a-folder-and-process-them
         */
        if(selection.equals("@")){
            File[] fileNames = directory.listFiles();
            for(File files : fileNames){
                File file = new File(directory, files.getName());
                file.delete();
            }
            return 0;
        }
        else if(selection.equals("*")){
            File[] fileNames = directory.listFiles();
            for(File files : fileNames){
                File file = new File(directory, files.getName());
                file.delete();
            }
            String msg = "@" + "_"+ "globaldelete";
            String result = clientTash_msg(msg);
            if(result.contains("0")){
                return 0;
            }
        }
        else {
            File file = new File(directory, selection);
            file.delete();
            return 0;
        }

        return  0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return "started";
    }

    @Override
    public  Uri  insert(Uri uri, ContentValues values) {
        System.out.println("In Insert "+ values.getAsString("key"));
        /*
        Forming the DHT with a node's predecessor and successor
         */
        if(global_cnt==0){
            global_cnt =1;
            String msg = "chord";
            String msg_ack= null;
            try {
                msg_ack = connectClient("chord");
                System.out.println("TThe string returned by Client " + msg_ack);
                if(msg_ack!=null){
                    String[] activenodes = msg_ack.split("\\+");
                    for (int i = 1; i < activenodes.length; i++) {
                        int avd = Integer.parseInt(activenodes[i]) / 2;
                        String emulator = Integer.toString(avd);
                        System.out.println(activenodes[i]);
                        String nodehash = genHash(emulator);
                        System.out.println("The avds are " + avd + " " + emulator + " " + activenodes[i] + " " + nodehash);
                        chordDHT.add(new nodeid(activenodes[i], nodehash));
                    }
                    Collections.sort(chordDHT, new sortbyhash());
                }

            }
            catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        avdno = Integer.parseInt(myPort)/2;
        try {
            myporthash = genHash(Integer.toString(avdno));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        System.out.println(" Objects created as follows: "+ myPort + " " + myporthash);
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        String hashkeycode = null;
        FileOutputStream outputStream;
        System.out.println("Printing the value in total node"+ chordDHT.size());
        System.out.println("The key value pair received is "+ key + value);
        if(chordDHT.size()>1){
            DHTsize = chordDHT.size();
            nodeid obj = new nodeid (myPort, myporthash);
            currindex = chordDHT.indexOf(obj);
            if(currindex == 0){
                previndex =DHTsize-1;
            }
            else{
                previndex =currindex-1;
            }
            if(currindex == DHTsize-1){
                succindex =0;
            }
            else{
                succindex =currindex+1;
            }
            System.out.println("Reached here to set index" + previndex + " " + currindex  + " " + succindex);
        }
        if(DHTsize>1){
            try {
                hashkeycode = genHash(key);
                if(hashkeycode.compareTo(chordDHT.get(DHTsize-1).getnodehash())>0){
                }
                System.out.println("Comparison result with prev" + hashkeycode.compareTo(chordDHT.get(previndex).getnodehash()));
                System.out.println("Comparision with curr" + hashkeycode.compareTo(chordDHT.get(currindex).getnodehash()));
                if((hashkeycode.compareTo(chordDHT.get(previndex).getnodehash())>0
                        && hashkeycode.compareTo(chordDHT.get(currindex).getnodehash())<=0) ||
                        (currindex==0 && hashkeycode.compareTo(chordDHT.get(currindex).getnodehash())<=0) ||
                        (currindex==0 && hashkeycode.compareTo(chordDHT.get(previndex).getnodehash())>0 )){
                                    /*
                    Referenced from:
                    https://developer.android.com/training/data-storage/files#java
                     */
                    System.out.println("Reached the correct node");
                    outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    outputStream.write(value.getBytes());
                    outputStream.close();
                }
                else{
                    String msg = key+"_"+value + "_"+ "prevnode";
                    String result= clientTash_msg(msg);

                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return uri;
        }
        else{
            try {
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();

            } catch (Exception e) {
                e.printStackTrace();
            }

            Log.v("insert", values.toString());
            return uri;
        }
    }

    @Override
    public boolean onCreate() {

        Log.d(TAG, "onCreate provider");

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        System.out.println("My Port is "+ myPort);
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            String msg = myPort+"_"+"participating";
            String res =new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort).get();
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public  Cursor  query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        if(global_cnt==0){
            global_cnt =2;
            String msg = "chord";
            String msg_ack= null;
            try {
                msg_ack = connectClient("chord");
                System.out.println("TThe string returned by Client " + msg_ack);
                if(msg_ack!=null){
                    String[] activenodes = msg_ack.split("\\+");
                    for (int i = 1; i < activenodes.length; i++) {
                        int avd = Integer.parseInt(activenodes[i]) / 2;
                        String emulator = Integer.toString(avd);
                        System.out.println(activenodes[i]);
                        String nodehash = genHash(emulator);
                        System.out.println("The avds are " + avd + " " + emulator + " " + activenodes[i] + " " + nodehash);
                        chordDHT.add(new nodeid(activenodes[i], nodehash));
                    }
                    Collections.sort(chordDHT, new sortbyhash());
                }

            }
            catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        String line = null;
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        File directory = getContext().getFilesDir();
        avdno = Integer.parseInt(myPort)/2;
        try {
            myporthash = genHash(Integer.toString(avdno));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        String hashkeycode = null;
        if(chordDHT.size()>1 && global_cnt==2){
            DHTsize = chordDHT.size();
            nodeid obj = new nodeid (myPort, myporthash);
            currindex = chordDHT.indexOf(obj);
            if(currindex == 0){
                previndex =DHTsize-1;
            }
            else{
                previndex =currindex-1;
            }
            if(currindex == DHTsize-1){
                succindex =0;
            }
            else{
                succindex =currindex+1;
            }
            System.out.println("Reached here to set index" + previndex + " " + currindex  + " " + succindex);
        }
        if(DHTsize>1){
            try {
                if(selection.length()>1){
                    hashkeycode = genHash(selection);
                    if((hashkeycode.compareTo(chordDHT.get(previndex).getnodehash())>0
                            && hashkeycode.compareTo(chordDHT.get(currindex).getnodehash())<=0) ||
                            (currindex==0 && hashkeycode.compareTo(chordDHT.get(currindex).getnodehash())<=0) ||
                            (currindex==0 && hashkeycode.compareTo(chordDHT.get(previndex).getnodehash())>0 )){

                        /*
                            Referenced from:
                            https://developer.android.com/training/data-storage/files#java
                            https://stackoverflow.com/questions/4716503/reading-a-plain-text-file-in-java
                            https://stackoverflow.com/questions/18368359/using-addrow-for-matrixcursor-how-to-add-different-object-types
                        */
                        File file = new File(directory, selection);
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                        line = bufferedReader.readLine();
                        cursor.addRow(new String[]{selection, line});
                        Log.v("query", selection);
                        Log.v("value", line);
                        return  cursor;
                    }
                    else{
                        String msg = selection + "_"+ "querynode";
                        String result = null;
                        if(hashkeycode.compareTo(chordDHT.get(DHTsize-1).getnodehash())>0 ||
                                hashkeycode.compareTo(chordDHT.get(0).getnodehash())<=0){
                            msg = msg + chordDHT.get(0).getport();
                        }
                        else{
                            for (int i=0;i<DHTsize;i++){
                                if(hashkeycode.compareTo(chordDHT.get(i).getnodehash())<=0){
                                    msg = msg + chordDHT.get(i).getport();
                                    break;
                                }
                            }

                        }

                        result = clientTash_msg(msg);
                        System.out.println("Client return :" +result);

                        if(result!=null){
                            String key_value [] = result.split("\\#");
                            for(int i=0;i<key_value.length;i++){
                                String key_str = key_value[i].substring(0,key_value[i].lastIndexOf("_"));
                                String value_str = key_value[i].substring(key_value[i].lastIndexOf("_")+1);
                                cursor.addRow(new String[]{key_str, value_str});
                            }
                            return cursor;
                        }

                    }
                }
                else if(selection.equals("@")){
                    File[] fileNames = directory.listFiles();
                    for(File files : fileNames){
                        File file = new File(directory, files.getName());
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                        line = bufferedReader.readLine();
                        cursor.addRow(new String[]{files.getName(), line});
                    }
                    return cursor;
                }
                else if(selection.equals("*")){
                    String msg = "@" + "_"+ "globaldump";
                    File[] fileNames = directory.listFiles();
                    for(File files : fileNames){
                        File file = new File(directory, files.getName());
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                        line = bufferedReader.readLine();
                        cursor.addRow(new String[]{files.getName(), line});
                    }

                    String result = clientTash_msg(msg);
                    System.out.println("Client return :" +result);
                    if(result!=null){
                        String key_value [] = result.split("\\#");
                        for(int i=0;i<key_value.length;i++){
                            String [] arr = key_value[i].split("\\_");
                            //String key_str = key_value[i].substring(0,key_value[i].lastIndexOf("_"));
                            //String value_str = key_value[i].substring(key_value[i].lastIndexOf("_")+1);
                            if(arr.length>1){
                                Log.v("PrintResult",key_value[i]+" "+arr[0]);
                                cursor.addRow(new String[]{arr[0], arr[1]});
                            }
                        }
                        return cursor;
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        else{
            try {
                if(selection.equals("@")|| selection.equals("*")){
                    File[] fileNames = directory.listFiles();
                    for(File files : fileNames){
                        File file = new File(directory, files.getName());
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                        line = bufferedReader.readLine();
                        cursor.addRow(new String[]{files.getName(), line});
                    }
                    return cursor;
                }
                else{
                    File file = new File(directory, selection);
                    BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                    line = bufferedReader.readLine();
                    cursor.addRow(new String[]{selection, line});
                    Log.v("query", selection);
                    Log.v("value", line);
                    return  cursor;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }



    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            Socket socket = null;

            try {
                while(true) {

                    /*
                     * Receiving msg from Client and passing them to onProgressUpdate
                     * Referenced from https://docs.oracle.com/javase/7/docs/api/java/io/ObjectInputStream.html
                     */
                    socket = serverSocket.accept();
                    InputStream inputStream = socket.getInputStream();
                    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                    String msg = (String) objectInputStream.readObject();

                    if(msg.contains("participating")){

                        msg = msg.substring(0,msg.indexOf("_"));
                        activenodes = activenodes + "+" + msg;
                        OutputStream outputStream= socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        String ack_msg= "ACK";
                        objectOutputStream.writeObject(ack_msg);
                        objectOutputStream.flush();
//                        activenodes = activenodes.substring(activenodes.indexOf("+")+1);
                        System.out.println("Reached here for counting participating nodes" + activenodes);
                    }
                    else if(msg.contains("chord")){
                        System.out.println("The activenodes are "+ activenodes);

                        OutputStream outputStream= socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        String ack_msg = activenodes;
                        objectOutputStream.writeObject(ack_msg);
                        objectOutputStream.flush();
                    }

                    else if(msg.contains("_prevnode")){
                        msg = msg.substring(0, msg.lastIndexOf("_"));
                        System.out.println("Received insert request" + msg);
                        String key = msg.substring(0,msg.lastIndexOf("_"));
                        String value = msg.substring(msg.lastIndexOf("_")+1);
                        ContentValues cv = new ContentValues();
                        cv.put("key", key);
                        cv.put("value", value);
                        insert(mUri, cv);
                        OutputStream outputStream= socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        String ack_msg= "ACK";
                        objectOutputStream.writeObject(ack_msg);
                        objectOutputStream.flush();
                        }
                    else if(msg.contains("querynode") || msg.contains("globaldump")){
                        String key;
                        System.out.println("Received query request " + msg);
                        if(msg.contains("querynode")){
                            key = msg.substring(0,msg.lastIndexOf("_"));
                        }
                        else {
                            key ="@";
                        }

                        Cursor resultCursor = query(mUri, null,
                                key, null, null);
                        /*
                        Referenced from :
                        https://stackoverflow.com/questions/4920528/iterate-through-rows-from-sqlite-query/4920643
                         */
                        String array[] = new String[resultCursor.getCount()];
                        int i = 0;
                        resultCursor.moveToFirst();
                        while (!resultCursor.isAfterLast()) {

                            String res_key = resultCursor.getString(resultCursor.getColumnIndex("key"));
                            String res_value = resultCursor.getString(resultCursor.getColumnIndex("value"));
                            String res_key_value = res_key + "_"+ res_value;
                            array[i] = res_key_value;
                            i++;
                            resultCursor.moveToNext();
                        }
                        /*
                        https://stackoverflow.com/questions/33802971/alternative-for-string-join-in-android
                         */
                        String result = TextUtils.join("#", array);
                        Log.v("Array",result);
                        OutputStream outputStream= socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(result);
                        objectOutputStream.flush();
                        }
                    else if(msg.contains("globaldelete")){
                        String selection = msg.substring(0,msg.lastIndexOf("_"));
                        int res  = delete(mUri, selection, null);

                        OutputStream outputStream= socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(Integer.toString(res));
                        objectOutputStream.flush();
                    }
                }
            }
            catch (Exception e) {
               e.printStackTrace();
            }
            finally {
                try {
                    socket.close();
                }
                catch (IOException e)
                {
                    System.out.println(e);
                }
            }
            return null;
        }

    }

    public String connectClient(String type){
        String msgToSend = type;
        String msg_ack ;
        try {
            /*
             * Sending message to server
             * Referenced from https://docs.oracle.com/javase/7/docs/api/java/io/ObjectOutputStream.html
             */
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));

            OutputStream outputStream = socket.getOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
            objectOutputStream.writeObject(msgToSend);
            objectOutputStream.flush();
            /*
             * Checking if acknowledgement message is received from  server
             * so that socket can be closed
             * Referenced from https://stackoverflow.com/questions/47903053/how-can-i-get-acknowledgment-at-client-side-without-closing-server-socket
             */
            InputStream inputStream = socket.getInputStream();
            ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
            msg_ack = (String) objectInputStream.readObject();
            if (msg_ack.contains("ACK")) {
                socket.close();

            }
            else if(msg_ack.contains("+")){
                System.out.println("The msg ack is " + msg_ack);
                socket.close();
                return msg_ack;
            }
        } catch (SocketTimeoutException e){
            System.out.println(e);
        } catch (Exception e) {
            System.out.print(e);
        }
        return null;
    }

    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected synchronized String doInBackground(String... msgs) {
            String msgToSend = msgs[0];
            String msg_ack ;
            try {
                /*
                 * Sending message to server
                 * Referenced from https://docs.oracle.com/javase/7/docs/api/java/io/ObjectOutputStream.html
                 */
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
                //socket.setSoTimeout(500);
                OutputStream outputStream = socket.getOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                objectOutputStream.writeObject(msgToSend);
                objectOutputStream.flush();
                /*
                 * Checking if acknowledgement message is received from  server
                 * so that socket can be closed
                 * Referenced from https://stackoverflow.com/questions/47903053/how-can-i-get-acknowledgment-at-client-side-without-closing-server-socket
                 */
                InputStream inputStream = socket.getInputStream();
                ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                msg_ack = (String) objectInputStream.readObject();
                if (msg_ack.contains("ACK")) {
                    socket.close();

                }
                else if(msg_ack.contains("+")){
                    System.out.println("The msg ack is " + msg_ack);
                    socket.close();
                    return msg_ack;
                }
            } catch (SocketTimeoutException e){
                System.out.println(e);
            } catch (Exception e) {
                System.out.print(e);
            }
            return null;
        }
    }
    public String clientTash_msg(String msg){
        String msgToSend = msg;
        String msg_ack = null;
        String msg_ack1 = "";
        int res =0;
        try{
            if(msgToSend.contains("prevnode")){
                System.out.println("Messages forwareded to successor" +chordDHT.get(succindex).getport());
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(succindex).getport()));
                OutputStream outputStream = socket.getOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                objectOutputStream.writeObject(msgToSend);
                objectOutputStream.flush();

                InputStream inputStream = socket.getInputStream();
                ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                msg_ack = (String) objectInputStream.readObject();
                    socket.close();
            }
            else if(msgToSend.contains("querynode")){
                System.out.println("Messages queried to successor" +msgToSend.substring(msgToSend.length() - 5));
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgToSend.substring(msgToSend.length() - 5)));
                OutputStream outputStream = socket.getOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                objectOutputStream.writeObject(msgToSend);
                objectOutputStream.flush();

                InputStream inputStream = socket.getInputStream();
                ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                msg_ack = (String) objectInputStream.readObject();
                return msg_ack;
            }
            else if(msgToSend.contains("globaldump")){
                for(int i=0;i<chordDHT.size();i++){
                    if(!chordDHT.get(i).getport().equals(myPort)){
                        System.out.println("Sending msg "+ msgToSend + " " + chordDHT.size() +" " +chordDHT.get(i).getport());
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(i).getport()));
                        OutputStream outputStream = socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(msgToSend);
                        objectOutputStream.flush();

                        InputStream inputStream = socket.getInputStream();
                        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                        msg_ack1 += (String) objectInputStream.readObject();
                        msg_ack1+="#";
                        socket.close();

                    }

                }
                return msg_ack1;
                }
            else if(msgToSend.contains("globaldelete")){
                for(int i=0;i<chordDHT.size();i++){
                    if(!chordDHT.get(i).getport().equals(myPort)){

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(chordDHT.get(i).getport()));
                        OutputStream outputStream = socket.getOutputStream();
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
                        objectOutputStream.writeObject(msgToSend);
                        objectOutputStream.flush();

                        InputStream inputStream = socket.getInputStream();
                        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                        msg_ack = (String) objectInputStream.readObject();
                        res = res + Integer.parseInt(msg_ack);
                        socket.close();
                    }
                }
                if(res==0) {
                    return "0";
                }
            }
        } catch (NullPointerException e){
            e.printStackTrace();
        } catch (Exception e) {
            System.out.print(e);
        }

        return null;
    }

}



