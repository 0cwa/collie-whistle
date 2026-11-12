package dev.whistle.receiver;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.util.Log;
import java.security.MessageDigest;

/** A UnifiedPush connector that logs the endpoint and the SHA-256 of every payload it is sent. */
public class Receiver extends BroadcastReceiver {
    @Override
    public void onReceive(Context context, Intent intent) {
        String action = intent.getAction();
        if ("org.unifiedpush.android.connector.NEW_ENDPOINT".equals(action)) {
            Log.i("WHISTLE_RX", "ENDPOINT " + intent.getStringExtra("endpoint"));
        } else if ("org.unifiedpush.android.connector.MESSAGE".equals(action)) {
            byte[] b = intent.getByteArrayExtra("bytesMessage");
            if (b == null) {
                Log.i("WHISTLE_RX", "MESSAGE none");
                return;
            }
            try {
                byte[] d = MessageDigest.getInstance("SHA-256").digest(b);
                StringBuilder hex = new StringBuilder();
                for (byte x : d) hex.append(String.format("%02x", x));
                Log.i("WHISTLE_RX", "MESSAGE len=" + b.length + " sha256=" + hex);
            } catch (Exception e) {
                Log.e("WHISTLE_RX", "hash failed", e);
            }
        }
    }
}
