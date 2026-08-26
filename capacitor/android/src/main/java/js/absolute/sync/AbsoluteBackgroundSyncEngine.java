package js.absolute.sync;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import com.getcapacitor.JSObject;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Arrays;
import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import android.util.Base64;
import js.absolute.devices.AbsoluteSecureStorageVault;
import org.json.JSONArray;
import org.json.JSONObject;

final class AbsoluteBackgroundSyncEngine {
    static final class NoSession extends Exception {}
    private static final int MAX_RESPONSE_BYTES = 5 * 1024 * 1024;
    private static final String DATA_KEY="absolutejs.sync.data-key.v1";
    private static final String RUNNING="running", LAST_RUN="lastRunAt", LAST_ERROR="lastError", LAST_ACK="lastAcknowledged", LAST_PULL="lastPulled";
    private static final class Mutation { final String id; final int attempts; Mutation(String i,int a){id=i;attempts=a;} }
    private static final class Batch { final JSONObject request=new JSONObject(); final Map<String,Mutation> mutations=new LinkedHashMap<>(); final Map<String,JSONObject> pulls=new LinkedHashMap<>(); }

    static JSObject status(Context context){
        android.content.SharedPreferences p=AbsoluteBackgroundSyncConfig.preferences(context); JSObject v=new JSObject();
        v.put("configured",p.contains(AbsoluteBackgroundSyncConfig.CONFIG)); v.put(RUNNING,p.getBoolean(RUNNING,false));
        if(p.contains(LAST_RUN))v.put(LAST_RUN,p.getLong(LAST_RUN,0)); if(p.contains(LAST_ERROR))v.put(LAST_ERROR,p.getString(LAST_ERROR,null));
        if(p.contains(LAST_ACK))v.put(LAST_ACK,p.getInt(LAST_ACK,0)); if(p.contains(LAST_PULL))v.put(LAST_PULL,p.getInt(LAST_PULL,0)); return v;
    }
    static void recordError(Context c,Exception e){AbsoluteBackgroundSyncConfig.preferences(c).edit().putBoolean(RUNNING,false).putString(LAST_ERROR,e.getMessage()==null?e.getClass().getSimpleName():e.getMessage()).apply();}
    static void run(Context context,AbsoluteBackgroundSyncConfig config)throws Exception{
        AbsoluteBackgroundSyncConfig.preferences(context).edit().putBoolean(RUNNING,true).remove(LAST_ERROR).apply();
        try{
            String access=refresh(context,config); SQLiteDatabase db=SQLiteDatabase.openDatabase(context.getDatabasePath(config.databaseName+"SQLite.db").getPath(),null,SQLiteDatabase.OPEN_READWRITE);
            try{db.execSQL("PRAGMA busy_timeout=5000"); Batch batch=prepare(context,db,config); JSONObject response=request(config.endpoint,"POST",batch.request.toString().getBytes(StandardCharsets.UTF_8),"application/json","Bearer "+access,config.issuer); int[] counts=settle(context,db,config,batch,response);
                AbsoluteBackgroundSyncConfig.preferences(context).edit().putBoolean(RUNNING,false).putLong(LAST_RUN,System.currentTimeMillis()).putInt(LAST_ACK,counts[0]).putInt(LAST_PULL,counts[1]).remove(LAST_ERROR).apply();
            }finally{db.close();}
        }catch(NoSession e){AbsoluteBackgroundSyncConfig.preferences(context).edit().putBoolean(RUNNING,false).remove(LAST_ERROR).apply();throw e;}
        catch(Exception e){recordError(context,e);throw e;}
    }
    private static String refresh(Context context,AbsoluteBackgroundSyncConfig config)throws Exception{
        String lease=AbsoluteSecureStorageVault.acquireLease(AbsoluteBackgroundSyncConfig.REFRESH_KEY,120000);if(lease==null)throw new IllegalStateException("Credential refresh is already running.");
        try{
            String refresh=AbsoluteSecureStorageVault.get(context,AbsoluteBackgroundSyncConfig.REFRESH_KEY);if(refresh==null)throw new NoSession();
            JSONObject discovery=request(config.issuer.replaceAll("/$","")+"/.well-known/openid-configuration","GET",null,null,null,config.issuer,false); String tokenEndpoint=discovery.getString("token_endpoint");
            String body=form("client_id",config.clientId)+"&"+form("grant_type","refresh_token")+"&"+form("refresh_token",refresh)+"&"+form("resource",config.issuer);
            JSONObject tokens=request(tokenEndpoint,"POST",body.getBytes(StandardCharsets.UTF_8),"application/x-www-form-urlencoded",null,config.issuer,true);
            if(!AbsoluteSecureStorageVault.setIfLease(context,AbsoluteBackgroundSyncConfig.REFRESH_KEY,tokens.getString("refresh_token"),lease))throw new NoSession();return tokens.getString("access_token");
        }finally{AbsoluteSecureStorageVault.releaseLease(AbsoluteBackgroundSyncConfig.REFRESH_KEY,lease);}
    }
    private static String form(String n,String v)throws Exception{return URLEncoder.encode(n,"UTF-8")+"="+URLEncoder.encode(v,"UTF-8");}

    private static Batch prepare(Context context,SQLiteDatabase db,AbsoluteBackgroundSyncConfig config)throws Exception{
        Batch b=new Batch(); JSONArray mutations=new JSONArray(),pulls=new JSONArray(); long now=System.currentTimeMillis(); db.beginTransaction();
        try{
            try(Cursor c=db.rawQuery("SELECT operation_id,record_json FROM absolute_sync_mutations WHERE namespace=? ORDER BY created_at,operation_id",new String[]{config.namespace})){
                while(c.moveToNext()&&mutations.length()<config.maxMutations){String id=c.getString(0);JSONObject r=decodeRecord(context,c.getString(1),"mutation",config.namespace);if("dead-letter".equals(r.optString("state"))||r.optLong("nextAttemptAt",0)>now)continue;int attempts=r.optInt("attempts",0)+1;r.put("attempts",attempts).remove("nextAttemptAt");updateMutation(context,db,config.namespace,id,r);JSONObject m=new JSONObject().put("operationId",id).put("name",r.getString("name"));if(r.has("args"))m.put("args",r.get("args"));mutations.put(m);b.mutations.put(id,new Mutation(id,attempts));}
            }
            try(Cursor c=db.rawQuery("SELECT collection_key,record_json FROM absolute_sync_collections WHERE namespace=? ORDER BY collection_key",new String[]{config.namespace})){
                while(c.moveToNext()&&pulls.length()<config.maxPulls){String key=c.getString(0);JSONObject r=decodeRecord(context,c.getString(1),"collection",config.namespace);String collection=r.optString("collection");if(collection.isEmpty())continue;JSONObject p=new JSONObject().put("id",key).put("collection",collection);if(r.has("params"))p.put("params",r.get("params"));if("id".equals(r.optString("headlessKey"))){if(r.has("cursor"))p.put("since",r.get("cursor"));else if(r.optInt("version",0)>0)p.put("since",r.getInt("version"));}pulls.put(p);b.pulls.put(key,r);}
            }
            db.setTransactionSuccessful();
        }finally{db.endTransaction();}
        b.request.put("version",1).put("mutations",mutations).put("pulls",pulls);return b;
    }
    private static int[] settle(Context context,SQLiteDatabase db,AbsoluteBackgroundSyncConfig config,Batch b,JSONObject response)throws Exception{
        if(response.getInt("version")!=1)throw new IllegalStateException("Unsupported Headless Sync response.");Map<String,JSONObject> mutationResults=index(response.getJSONArray("mutations"),"operationId"),pullResults=index(response.getJSONArray("pulls"),"id");int ack=0,pulled=0;db.beginTransaction();
        try{
            for(Mutation sent:b.mutations.values()){JSONObject current=record(context,db,"absolute_sync_mutations","operation_id","mutation",config.namespace,sent.id);if(current==null||current.optInt("attempts",-1)!=sent.attempts)continue;JSONObject outcome=mutationResults.get(sent.id);if(outcome!=null&&"ack".equals(outcome.optString("status"))){db.delete("absolute_sync_mutations","namespace=? AND operation_id=?",new String[]{config.namespace,sent.id});ack++;continue;}JSONObject rejection=outcome==null?new JSONObject().put("kind","retryable").put("message","Missing mutation response."):outcome.getJSONObject("rejection");current.put("lastError",rejection.optString("message","Mutation rejected.")).put("rejection",rejection);JSONObject conflictPolicy=current.optJSONObject("conflictPolicy");String conflictStrategy=conflictPolicy==null?"manual":conflictPolicy.optString("strategy","manual");if("conflict".equals(rejection.optString("kind"))&&"server-wins".equals(conflictStrategy)){db.delete("absolute_sync_mutations","namespace=? AND operation_id=?",new String[]{config.namespace,sent.id});continue;}if("conflict".equals(rejection.optString("kind"))&&"client-wins".equals(conflictStrategy)&&current.optInt("conflictAttempts",0)<conflictPolicy.optInt("maxAttempts",1)){current.put("conflictAttempts",current.optInt("conflictAttempts",0)+1).put("state","pending").remove("nextAttemptAt");updateMutation(context,db,config.namespace,sent.id,current);continue;}if(!"retryable".equals(rejection.optString("kind"))||sent.attempts>=config.maxAttempts)current.put("state","dead-letter").put("deadLetteredAt",System.currentTimeMillis()).remove("nextAttemptAt");else current.put("state","pending").put("nextAttemptAt",System.currentTimeMillis()+Math.max(0,rejection.optLong("retryAfterMs",Math.min(30000,500L<<Math.min(20,sent.attempts-1)))));updateMutation(context,db,config.namespace,sent.id,current);}
            for(Map.Entry<String,JSONObject> requested:b.pulls.entrySet()){JSONObject result=pullResults.get(requested.getKey());if(result==null||"error".equals(result.optString("type")))continue;JSONObject current=record(context,db,"absolute_sync_collections","collection_key","collection",config.namespace,requested.getKey());if(!same(current,requested.getValue()))continue;ContentValues values=new ContentValues();JSONObject next=applyPull(current,result);values.put("record_json",encodeRecord(context,next,"collection",config.namespace,next.optString("collection",requested.getKey())));db.update("absolute_sync_collections",values,"namespace=? AND collection_key=?",new String[]{config.namespace,requested.getKey()});pulled++;}
            db.setTransactionSuccessful();
        }finally{db.endTransaction();}return new int[]{ack,pulled};
    }
    private static JSONObject applyPull(JSONObject current,JSONObject result)throws Exception{JSONObject next=new JSONObject(current.toString());if("snapshot".equals(result.getString("type")))next.put("rows",result.getJSONArray("rows"));else{Map<String,JSONObject> rows=new LinkedHashMap<>();JSONArray existing=current.getJSONArray("rows");for(int i=0;i<existing.length();i++){JSONObject row=existing.getJSONObject(i);rows.put(key(row),row);}remove(rows,result.getJSONArray("removed"));set(rows,result.getJSONArray("changed"));set(rows,result.getJSONArray("added"));next.put("rows",new JSONArray(rows.values()));}return next.put("version",result.getInt("version")).put("cursor",result.getString("cursor"));}
    private static String key(JSONObject row)throws Exception{Object id=row.get("id");if(!(id instanceof String)&&!(id instanceof Number))throw new IllegalStateException("Background Sync row requires id.");return id.getClass().getName()+":"+id;}
    private static void set(Map<String,JSONObject> rows,JSONArray values)throws Exception{for(int i=0;i<values.length();i++){JSONObject row=values.getJSONObject(i);rows.put(key(row),row);}}
    private static void remove(Map<String,JSONObject> rows,JSONArray values)throws Exception{for(int i=0;i<values.length();i++)rows.remove(key(values.getJSONObject(i)));}
    private static boolean same(JSONObject a,JSONObject b){return a!=null&&a.optInt("version")==b.optInt("version")&&a.optString("cursor").equals(b.optString("cursor"));}
    private static Map<String,JSONObject> index(JSONArray values,String field)throws Exception{Map<String,JSONObject> result=new HashMap<>();for(int i=0;i<values.length();i++){JSONObject value=values.getJSONObject(i);result.put(value.getString(field),value);}return result;}
    private static JSONObject record(Context context,SQLiteDatabase db,String table,String field,String kind,String namespace,String id)throws Exception{try(Cursor c=db.rawQuery("SELECT record_json FROM "+table+" WHERE namespace=? AND "+field+"=? LIMIT 1",new String[]{namespace,id})){return c.moveToFirst()?decodeRecord(context,c.getString(0),kind,namespace):null;}}
    private static void updateMutation(Context context,SQLiteDatabase db,String namespace,String id,JSONObject record)throws Exception{ContentValues values=new ContentValues();values.put("record_json",encodeRecord(context,record,"mutation",namespace,record.getString("name")));db.update("absolute_sync_mutations",values,"namespace=? AND operation_id=?",new String[]{namespace,id});}
    private static byte[] dataKey(Context context)throws Exception{String value=AbsoluteSecureStorageVault.get(context,DATA_KEY);if(value==null)return null;byte[] key=Base64.decode(value,Base64.NO_WRAP);if(key.length!=32)throw new IllegalStateException("Protected Sync data key is invalid.");return key;}
    private static String aad(String kind,String namespace,String name){return "absolute-sync-v1\u0000"+kind+"\u0000"+namespace+"\u0000"+name;}
    private static JSONObject decodeRecord(Context context,String stored,String kind,String namespace)throws Exception{JSONObject outer=new JSONObject(stored);if(!outer.has("__absoluteSyncProtected"))return outer;JSONObject envelope=outer.getJSONObject("__absoluteSyncProtected");if(!"aes-256-gcm-v1".equals(envelope.getString("protector")))throw new IllegalStateException("Unsupported Sync data protector.");byte[] key=dataKey(context);if(key==null)throw new IllegalStateException("Protected Sync data key is unavailable.");byte[] packed=Base64.decode(envelope.getString("value"),Base64.NO_WRAP),nonce=Arrays.copyOfRange(packed,0,12),ciphertext=Arrays.copyOfRange(packed,12,packed.length);Cipher cipher=Cipher.getInstance("AES/GCM/NoPadding");cipher.init(Cipher.DECRYPT_MODE,new SecretKeySpec(key,"AES"),new GCMParameterSpec(128,nonce));cipher.updateAAD(aad(kind,namespace,envelope.getString("name")).getBytes(StandardCharsets.UTF_8));return new JSONObject(new String(cipher.doFinal(ciphertext),StandardCharsets.UTF_8));}
    private static String encodeRecord(Context context,JSONObject record,String kind,String namespace,String name)throws Exception{byte[] key=dataKey(context);if(key==null)return record.toString();byte[] nonce=new byte[12];new SecureRandom().nextBytes(nonce);Cipher cipher=Cipher.getInstance("AES/GCM/NoPadding");cipher.init(Cipher.ENCRYPT_MODE,new SecretKeySpec(key,"AES"),new GCMParameterSpec(128,nonce));cipher.updateAAD(aad(kind,namespace,name).getBytes(StandardCharsets.UTF_8));byte[] ciphertext=cipher.doFinal(record.toString().getBytes(StandardCharsets.UTF_8)),packed=new byte[nonce.length+ciphertext.length];System.arraycopy(nonce,0,packed,0,nonce.length);System.arraycopy(ciphertext,0,packed,nonce.length,ciphertext.length);return new JSONObject().put("__absoluteSyncProtected",new JSONObject().put("name",name).put("protector","aes-256-gcm-v1").put("value",Base64.encodeToString(packed,Base64.NO_WRAP))).toString();}

    private static JSONObject request(String address,String method,byte[] body,String type,String auth,String trustedOrigin)throws Exception{return request(address,method,body,type,auth,trustedOrigin,false);}
    private static JSONObject request(String address,String method,byte[] body,String type,String auth,String trustedOrigin,boolean advertisedTokenEndpoint)throws Exception{
        if(body!=null&&body.length>MAX_RESPONSE_BYTES)throw new IllegalStateException("Background request is too large.");URI target=new URI(address);if(advertisedTokenEndpoint){boolean loopback="http".equals(target.getScheme())&&("localhost".equalsIgnoreCase(target.getHost())||"127.0.0.1".equals(target.getHost())||"::1".equals(target.getHost()));if((!"https".equals(target.getScheme())&&!loopback)||target.getUserInfo()!=null||target.getFragment()!=null)throw new SecurityException("OIDC advertised an unsafe token endpoint.");}else if(!sameOrigin(target,new URI(trustedOrigin)))throw new SecurityException("Background request left the Auth issuer origin.");HttpURLConnection c=(HttpURLConnection)new URL(address).openConnection();c.setConnectTimeout(10000);c.setReadTimeout(20000);c.setRequestMethod(method);c.setRequestProperty("Accept","application/json");c.setInstanceFollowRedirects(false);if(auth!=null)c.setRequestProperty("Authorization",auth);if(body!=null){c.setDoOutput(true);c.setFixedLengthStreamingMode(body.length);c.setRequestProperty("Content-Type",type);try(OutputStream output=c.getOutputStream()){output.write(body);}}int status=c.getResponseCode();InputStream input=status>=200&&status<300?c.getInputStream():c.getErrorStream();byte[] bytes=read(input);c.disconnect();if(status<200||status>=300)throw new IllegalStateException("Background request failed with HTTP "+status+".");return new JSONObject(new String(bytes,StandardCharsets.UTF_8));
    }
    private static boolean sameOrigin(URI a,URI b){int ap=a.getPort()<0?("https".equals(a.getScheme())?443:80):a.getPort(),bp=b.getPort()<0?("https".equals(b.getScheme())?443:80):b.getPort();return a.getScheme().equals(b.getScheme())&&a.getHost().equalsIgnoreCase(b.getHost())&&ap==bp;}
    private static byte[] read(InputStream input)throws Exception{if(input==null)return new byte[0];ByteArrayOutputStream out=new ByteArrayOutputStream();byte[] buffer=new byte[8192];int count,total=0;while((count=input.read(buffer))!=-1){total+=count;if(total>MAX_RESPONSE_BYTES)throw new IllegalStateException("Background response is too large.");out.write(buffer,0,count);}input.close();return out.toByteArray();}
}
