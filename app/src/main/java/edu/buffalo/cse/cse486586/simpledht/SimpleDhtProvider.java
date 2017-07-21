package edu.buffalo.cse.cse486586.simpledht;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import android.os.AsyncTask;
import android.os.Bundle;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.ContextWrapper;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.util.HashMap;

import static android.provider.CalendarContract.CalendarCache.URI;


public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    public String mySuccesorID = new String();
    public String myPredecessorID = new String();
    public String myHashID = new String();
    public String myPortID = new String();
    public ArrayList myKeys = new ArrayList();


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String filename = selection;
        File filesDir = getContext().getFilesDir();

        if(filename.equals("@")){
            for(int i = 0; i < myKeys.size(); i++) {
                String tempValue = (String) myKeys.get(i);
                File fileToDelete = new File(filesDir, tempValue);
            }
        }
        else{
            File fileToDelete = new File(filesDir,filename);
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String filename = (String) values.get("key");
        String value = (String) values.get("value");
        String keyHashValue = new String();
        String predHashValue = new String();

        try{
            keyHashValue = genHash(filename);
            predHashValue = genHash(myPredecessorID);
        }
        catch (Exception e){
            Log.e(TAG,"keyHashValue generation problem");
        }
        int tempComparedKP = keyHashValue.compareTo(predHashValue);
        int tempComparedKM = keyHashValue.compareTo(myHashID);
        int tempComparedMP = myHashID.compareTo(predHashValue);

        if(myPortID.equals(mySuccesorID)){
            Context context = getContext();
            try{
                myKeys.add(filename);
                FileOutputStream fos = context.openFileOutput(filename,Context.MODE_PRIVATE);
                fos.write(value.getBytes());
                fos.close();
            }catch(Exception e){
                e.printStackTrace();
            }

        }

        else if((tempComparedKP > 0)&&(tempComparedKM <= 0)){
            Context context = getContext();
            try{
                myKeys.add(filename);
                FileOutputStream fos = context.openFileOutput(filename,Context.MODE_PRIVATE);
                fos.write(value.getBytes());
                fos.close();
            }catch(Exception e){
                e.printStackTrace();
            }

        }
        else if((tempComparedMP < 0) && ((tempComparedKP > 0)||(tempComparedKM <= 0))){
                Context context = getContext();
                try{
                    myKeys.add(filename);
                    FileOutputStream fos = context.openFileOutput(filename,Context.MODE_PRIVATE);
                    fos.write(value.getBytes());
                    fos.close();
                }catch(Exception e){
                    e.printStackTrace();
                }
        }
        else{

            String msg = "3KV" + "," + filename + "," + value;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        }

        return uri;
    }

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf(Integer.parseInt(portStr));

        try{
            myPortID = myPort;
            myHashID = genHash(myPort);
            myPredecessorID = myPortID;
            mySuccesorID = myPortID;
        }
        catch (Exception e){
            Log.e(TAG,"NoSuchAlgorithm: genHash");
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        if(!(myPortID.equals("5554"))){
            String msg = "1NN" + "," + myPortID;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            return false;
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        Context context = getContext();
        String filename = selection;
        String[] columnames = {"key","value"};
        MatrixCursor matrixCursor = new MatrixCursor(columnames);

        if(filename.equals("*")){
            String valStr = new String();
            String keyStr = new String();
            if(!(myPortID.equals(mySuccesorID))){
                String msgQuery = "5Q" + "," + myPortID + "," + "@";
                try{
                    Socket socketA = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            (Integer.parseInt(mySuccesorID))*2);
                    DataOutputStream msgoutA = new DataOutputStream(socketA.getOutputStream());
                    DataInputStream msginA = new DataInputStream(socketA.getInputStream());
                    msgoutA.writeUTF(msgQuery);
                    String msgreadA = msginA.readUTF();
                    String[] msgreadAsplitted = msgreadA.split(",");
                    while(!msgreadAsplitted[0].equals("msgrcvd")){
                    }
                    msginA.close();
                    msgoutA.close();
                    socketA.close();
                    if(!(msgreadAsplitted[1].equals("empty0"))){
                        keyStr = msgreadAsplitted[1];
                        valStr = msgreadAsplitted[2];
                    }
                }
                catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }

            }

            if(myKeys.size() > 0){
                for(int i = 0; i < myKeys.size(); i++){
                    String tempValue = (String) myKeys.get(i);
                    StringBuilder stringBuilder = new StringBuilder();
                    try {
                        String line;
                        FileInputStream fis = context.openFileInput(tempValue);
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
                        while((line = bufferedReader.readLine())!=null){
                            stringBuilder.append(line);
                        }
                        String value = new String();
                        value = stringBuilder.toString();
                        fis.close();
                        Object[] columnvalues = new Object[2];
                        columnvalues[0] = tempValue;
                        columnvalues[1] = value;
                        matrixCursor.addRow(columnvalues);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            if(!(keyStr.isEmpty())){
                String[] keyStrSplitted = keyStr.split(":");
                String[] valStrSplitted = valStr.split(":");
                if(keyStrSplitted.length==valStrSplitted.length){
                    for(int i = 1; i < keyStrSplitted.length; i++){
                        Object[] columnvalues = new Object[2];
                        columnvalues[0] = keyStrSplitted[i];
                        columnvalues[1] = valStrSplitted[i];
                        matrixCursor.addRow(columnvalues);
                    }
                }
                else{
                    Log.e(TAG,"length of key and value string does not match");
                }
            }
        }
        else if(filename.equals("@")){
            for(int i = 0; i < myKeys.size(); i++){
                String tempValue = (String) myKeys.get(i);
                StringBuilder stringBuilder = new StringBuilder();
                try {
                    String line;
                    FileInputStream fis = context.openFileInput(tempValue);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
                    while((line = bufferedReader.readLine())!=null){
                        stringBuilder.append(line);
                    }
                    String value = new String();
                    value = stringBuilder.toString();
                    fis.close();
                    Object[] columnvalues = new Object[2];
                    columnvalues[0] = tempValue;
                    columnvalues[1] = value;
                    matrixCursor.addRow(columnvalues);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
        else{
            String filenameHashed = new String();
            String predHashValue = new String();
            StringBuilder stringBuilder = new StringBuilder();
            try{
                filenameHashed = genHash(filename);
                predHashValue = genHash(myPredecessorID);
            }
            catch (Exception e){
                e.printStackTrace();
            }
            int tempComparedKP = filenameHashed.compareTo(predHashValue);
            int tempComparedKM = filenameHashed.compareTo(myHashID);
            int tempComparedMP = myHashID.compareTo(predHashValue);

            if(myPortID.equals(myPredecessorID)){
                String value = new String();
                try {
                    String line;
                    FileInputStream fis = context.openFileInput(filename);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
                    while((line = bufferedReader.readLine())!=null){
                        stringBuilder.append(line);
                    }

                    value = stringBuilder.toString();
                    fis.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                Object[] columnvalues = new Object[2];
                columnvalues[0] = filename;
                columnvalues[1] = value;
                matrixCursor.addRow(columnvalues);
            }

            else if((tempComparedKP > 0)&&(tempComparedKM <= 0)){
                String value = new String();
                try {
                    String line;
                    FileInputStream fis = context.openFileInput(filename);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
                    while((line = bufferedReader.readLine())!=null){
                        stringBuilder.append(line);
                    }
                    value = stringBuilder.toString();
                    fis.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                Object[] columnvalues = new Object[2];
                columnvalues[0] = filename;
                columnvalues[1] = value;
                matrixCursor.addRow(columnvalues);
            }
            else if((tempComparedMP < 0)&&((tempComparedKP > 0)||(tempComparedKM < 0))){
                    String value = new String();
                    try {
                        String line;
                        FileInputStream fis = context.openFileInput(filename);
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
                        while((line = bufferedReader.readLine())!=null){
                            stringBuilder.append(line);
                        }
                        value = stringBuilder.toString();
                        fis.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    Object[] columnvalues = new Object[2];
                    columnvalues[0] = filename;
                    columnvalues[1] = value;
                    matrixCursor.addRow(columnvalues);
            }
            else{
                try{
                    Socket socketQ = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),(
                            Integer.parseInt(mySuccesorID))*2);
                    DataInputStream queryVal = new DataInputStream(socketQ.getInputStream());
                    DataOutputStream queryOut = new DataOutputStream(socketQ.getOutputStream());
                    String filenameNew = "4Q" + "," + filename;
                    queryOut.writeUTF(filenameNew);
                    String valRcvd = queryVal.readUTF();
                    String[] valRcvdSplitted = valRcvd.split(",");
                    while (!valRcvdSplitted[0].equals("msgrcvd")){

                    }
                    String val = valRcvdSplitted[1];
                    queryVal.close();
                    queryOut.close();
                    socketQ.close();

                    Object[] columnvalues = new Object[2];
                    columnvalues[0] = filename;
                    columnvalues[1] = val;
                    matrixCursor.addRow(columnvalues);
                }
                catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException in query method");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException in query method");
                }
            }

        }
        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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
        HashMap<String,String> liveNodeHash = new HashMap();
        ArrayList liveNode = new ArrayList();
        Integer nodeCount = 0;

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            if(myPortID.equals("5554")){
                liveNode.add(myPortID);
                liveNodeHash.put(myPortID,myHashID);
                nodeCount = nodeCount + 1;
            }

            try{
                while(true){
                    Socket clientSocket = serverSocket.accept();
                    DataOutputStream msgout = new DataOutputStream(clientSocket.getOutputStream());
                    DataInputStream msgin = new DataInputStream(clientSocket.getInputStream());
                    String msgRead = msgin.readUTF();
                    String[] msgReadSplitted = msgRead.split(",");

                    if(msgReadSplitted[0].equals("1NN")){
                        String predToSend = new String();
                        String succToSend = new String();


                        try{
                            String newNode = msgReadSplitted[1];
                            String newHashID = genHash(newNode);

                            if((liveNode.size() + 1)==2){
                                predToSend = myPortID;
                                succToSend = myPortID;
                                myPredecessorID = newNode;
                                mySuccesorID = newNode;
                            }
                            else if((liveNode.size() + 1) > 2){
                                String minHashValue = Collections.min(liveNodeHash.values());
                                String maxHashValue = Collections.max(liveNodeHash.values());
                                String minLiveNode = new String();
                                String maxLiveNode = new String();

                                for(int i =0; i < liveNode.size(); i++ ){
                                    if(minHashValue.equals(liveNodeHash.get((String) liveNode.get(i)))){
                                        minLiveNode = (String) liveNode.get(i);
                                    }
                                    else if(maxHashValue.equals(liveNodeHash.get((String) liveNode.get(i)))){
                                        maxLiveNode = (String) liveNode.get(i);
                                    }
                                }

                                int maxComparedValue = newHashID.compareTo(maxHashValue);
                                int minComparedValue = newHashID.compareTo(minHashValue);
                                if((maxComparedValue > 0)||(minComparedValue < 0)){ // i
                                    predToSend = maxLiveNode;
                                    succToSend = minLiveNode;
                                }
                                else{
                                    predToSend = minLiveNode;
                                    succToSend = maxLiveNode;
                                    for(int i = 0; i < liveNode.size() ; i++){
                                        String tempValue = (String)liveNode.get(i);
                                        String tempHashValue = liveNodeHash.get(tempValue);
                                        String predHashValue = liveNodeHash.get(predToSend);
                                        String succHashValue = liveNodeHash.get(succToSend);
                                        int tempComparedTN = tempHashValue.compareTo(newHashID);
                                        int tempComparedTS = tempHashValue.compareTo(succHashValue);
                                        int tempComparedTP = tempHashValue.compareTo(predHashValue);
                                        if(tempComparedTN < 0){
                                            if(tempComparedTP >= 0){
                                                predToSend = tempValue;
                                            }
                                        }
                                        if(tempComparedTN > 0){
                                            if(tempComparedTS <= 0){
                                                succToSend = tempValue;;
                                            }
                                        }

                                    }
                                }
                            }

                            liveNode.add(newNode);
                            liveNodeHash.put(newNode,newHashID);

                            if((predToSend.equals(myPortID)&&(!(succToSend.equals(myPortID))))){
                                mySuccesorID = newNode;
                                Socket socketS = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        (Integer.parseInt(succToSend))*2);
                                DataOutputStream msgoutS = new DataOutputStream(socketS.getOutputStream());
                                DataInputStream msginS = new DataInputStream(socketS.getInputStream());
                                String msgToSendS = "2SP" + "," + "Pval" + "," + newNode;
                                msgoutS.writeUTF(msgToSendS);
                                String msginUTFS = msginS.readUTF();
                                msginS.close();
                                msgoutS.close();
                                while(!msginUTFS.equals("msgrcvd")){
                                }
                                socketS.close();
                            }
                            if((succToSend.equals(myPortID)&&(!(predToSend.equals(myPortID))))){
                                myPredecessorID = newNode;
                                Socket socketP = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        (Integer.parseInt(predToSend))*2);
                                DataOutputStream msgoutP = new DataOutputStream(socketP.getOutputStream());
                                DataInputStream msginP = new DataInputStream(socketP.getInputStream());
                                String msgToSendP = "2SP" + "," + "Sval" + "," + newNode;
                                msgoutP.writeUTF(msgToSendP);
                                String msginUTFP = msginP.readUTF();
                                msginP.close();
                                msgoutP.close();
                                while(!msginUTFP.equals("msgrcvd")){
                                }
                                socketP.close();
                            }
                            if((!(succToSend.equals(myPortID)))&&!(predToSend.equals(myPortID))){
                                Socket socketS = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        (Integer.parseInt(succToSend))*2);
                                DataOutputStream msgoutS = new DataOutputStream(socketS.getOutputStream());
                                DataInputStream msginS = new DataInputStream(socketS.getInputStream());
                                String msgToSendS = "2SP" + "," + "Pval" + "," + newNode;
                                msgoutS.writeUTF(msgToSendS);
                                String msginUTFS = msginS.readUTF();
                                msginS.close();
                                msgoutS.close();
                                while(!msginUTFS.equals("msgrcvd")){
                                }
                                socketS.close();
                                Socket socketP = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        (Integer.parseInt(predToSend))*2);
                                DataOutputStream msgoutP = new DataOutputStream(socketP.getOutputStream());
                                DataInputStream msginP = new DataInputStream(socketP.getInputStream());
                                String msgToSendP = "2SP" + "," + "Sval" + "," + newNode;
                                msgoutP.writeUTF(msgToSendP);
                                String msginUTFP = msginP.readUTF();
                                msginP.close();
                                msgoutP.close();
                                while(!msginUTFP.equals("msgrcvd")){
                                }
                                socketP.close();

                            }
                        }
                        catch (Exception e){
                            Log.e(TAG,"hashID at 5554 cannot be generated");
                        }

                        String msgToSend = "msgrcvd" + "," + predToSend + "," + succToSend;
                        msgout.writeUTF(msgToSend);
                        msgin.close();
                        msgout.close();
                        clientSocket.close();
                    }
                    else if(msgReadSplitted[0].equals("2SP")){
                        if(msgReadSplitted[1].equals("Pval")){
                            myPredecessorID = msgReadSplitted[2];
                        }
                        else if(msgReadSplitted[1].equals("Sval")){
                            mySuccesorID = msgReadSplitted[2];
                        }
                        msgout.writeUTF("msgrcvd");
                        msgin.close();
                        msgout.close();
                        clientSocket.close();

                    }
                    else if(msgReadSplitted[0].equals("3KV")){
                        String scheme = "content";
                        String authority = "edu.buffalo.cse.cse486586.simpledht.provider";
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority(authority);
                        uriBuilder.scheme(scheme);
                        Uri uri = uriBuilder.build();

                        String filename = msgReadSplitted[1];
                        String value = msgReadSplitted[2];
                        ContentValues contentValues = new ContentValues();
                        contentValues.put("key",filename);
                        contentValues.put("value",value);
                        ContentResolver contentResolver = getContext().getContentResolver();
                        contentResolver.insert(uri, contentValues);
                        msgout.writeUTF("msgrcvd");
                        msgin.close();
                        msgout.close();
                        clientSocket.close();
                    }

                    else if(msgReadSplitted[0].equals("4Q")){

                        String scheme = "content";
                        String authority = "edu.buffalo.cse.cse486586.simpledht.provider";
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority(authority);
                        uriBuilder.scheme(scheme);
                        Uri uri = uriBuilder.build();

                        String key = msgReadSplitted[1];

                        try{
                            ContentResolver contentResolver = getContext().getContentResolver();
                            Cursor resultCursor = contentResolver.query(uri, null,
                                    key, null, null);

                            if (resultCursor == null) {
                                Log.e(TAG, "Result null");
                                throw new Exception();
                            }
                            int keyIndex = resultCursor.getColumnIndex("key");
                            int valueIndex = resultCursor.getColumnIndex("value");
                            if (keyIndex == -1 || valueIndex == -1) {
                                Log.e(TAG, "Wrong columns");
                                resultCursor.close();
                                throw new Exception();
                            }

                            resultCursor.moveToFirst();

                            if (!(resultCursor.isFirst() && resultCursor.isLast())) {
                                Log.e(TAG, "Wrong number of rows");
                                resultCursor.close();
                                throw new Exception();
                            }

                            String returnValue = resultCursor.getString(valueIndex);

                            String queryReply = "msgrcvd" + "," + returnValue;
                            msgout.writeUTF(queryReply);
                            msgin.close();
                            msgout.close();
                            clientSocket.close();
                         }
                        catch (Exception e){
                            Log.e("4Q","problem in returning value from server");
                        }


                    }

                    else if(msgReadSplitted[0].equals("5Q")){
                        String portNum = msgReadSplitted[1];
                        String resKey = new String();
                        String resVal = new String();

                        if(!(mySuccesorID.equals(portNum))){
                            try{
                                Socket socketSA = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        (Integer.parseInt(mySuccesorID))*2);
                                DataOutputStream msgoutSA = new DataOutputStream(socketSA.getOutputStream());
                                DataInputStream msginSA = new DataInputStream(socketSA.getInputStream());
                                msgoutSA.writeUTF(msgRead);
                                String msgreadSA = msginSA.readUTF();
                                String[] msgreadSAsplitted = msgreadSA.split(",");
                                while(!msgreadSAsplitted[0].equals("msgrcvd")){
                                }

                                if(msgreadSAsplitted[1].equals("empty0")){
                                    Log.e(TAG,"msgreadSAsplitted[0] is: " + msgreadSAsplitted[0]);
                                    Log.e(TAG,"msgreadSAsplitted[1] is: " + msgreadSAsplitted[1]);
                                }
                                else{
                                    resKey = msgreadSAsplitted[1];
                                    resVal = msgreadSAsplitted[2];
                                }

                                msginSA.close();
                                msgoutSA.close();
                                socketSA.close();
                            }
                            catch (UnknownHostException e) {
                                Log.e(TAG, "ClientTask UnknownHostException");
                            } catch (IOException e) {
                                Log.e(TAG, "ClientTask socket IOException");
                            }
                        }

                        String scheme = "content";
                        String authority = "edu.buffalo.cse.cse486586.simpledht.provider";
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority(authority);
                        uriBuilder.scheme(scheme);
                        Uri uri = uriBuilder.build();

                        String key = msgReadSplitted[2];

                        try{
                            ContentResolver contentResolver = getContext().getContentResolver();
                            Cursor resultCursor = contentResolver.query(uri, null,
                                    key, null, null);

                            if (resultCursor == null) {
                                Log.e(TAG, "Result null in 5Q");
                                throw new Exception();
                            }
                            int keyIndex = resultCursor.getColumnIndex("key");
                            int valueIndex = resultCursor.getColumnIndex("value");
                            if (keyIndex == -1 || valueIndex == -1) {
                                Log.e(TAG, "Wrong columns in 5Q");
                                resultCursor.close();
                                throw new Exception();
                            }

                            int curSize = resultCursor.getCount();
                            String queryReply = "msgrcvd";
                            if(!(curSize==0)){
                                for(int m = 0; m < curSize; m++){
                                    resultCursor.moveToPosition(m);
                                    resKey = resKey + ":" + resultCursor.getString(keyIndex);
                                    resVal = resVal + ":" + resultCursor.getString(valueIndex);
                                }


                            }
                            if(!(resKey.isEmpty())){
                                queryReply = queryReply + "," + resKey + "," + resVal;
                            }
                            else{
                                queryReply = queryReply + "," + "empty0";
                            }

                            msgout.writeUTF(queryReply);
                            msgin.close();
                            msgout.close();
                            clientSocket.close();
                        }
                        catch (Exception e){
                            Log.e("5Q","problem not returning value from server");
                        }



                    }

                }

            }
            catch(IOException x){
                Log.e(TAG, "File read failed");
            }

            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String msg = msgs[0];
                String[] msgSplitted = msg.split(",");
                    Socket socket0 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            (Integer.parseInt("5554")*2));
                    DataOutputStream msgout0 = new DataOutputStream(socket0.getOutputStream());
                    DataInputStream msgin0 = new DataInputStream(socket0.getInputStream());
                    msgout0.writeUTF(msg);
                    String msgRead = msgin0.readUTF();
                    String[] msgReadSplitted = msgRead.split(",");
                    myPredecessorID = msgReadSplitted[1];
                    mySuccesorID = msgReadSplitted[2];
                    msgin0.close();
                    msgout0.close();
                    socket0.close();
                }

                if(msgSplitted[0].equals("3KV")){
                    Socket socketSc = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            (Integer.parseInt(mySuccesorID)*2));
                    DataOutputStream msgoutSc = new DataOutputStream(socketSc.getOutputStream());
                    DataInputStream msginSc = new DataInputStream(socketSc.getInputStream());
                    msgoutSc.writeUTF(msg);
                    String msgRead = msginSc.readUTF();
                    msginSc.close();
                    msgoutSc.close();
                    socketSc.close();
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }
}
