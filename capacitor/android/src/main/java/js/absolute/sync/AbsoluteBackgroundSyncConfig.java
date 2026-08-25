package js.absolute.sync;

import android.content.Context;
import android.content.SharedPreferences;
import java.net.URI;
import org.json.JSONException;
import org.json.JSONObject;

final class AbsoluteBackgroundSyncConfig {
    static final String PREFERENCES = "absolutejs-background-sync";
    static final String CONFIG = "config";
    static final String WORK_NAME = "absolutejs-background-sync";
    static final String REFRESH_KEY = "absolutejs.auth.oidc.refresh";
    final String endpoint, issuer, clientId, namespace, databaseName;
    final int intervalMinutes, maxMutations, maxPulls, maxAttempts;

    AbsoluteBackgroundSyncConfig(JSONObject value) throws Exception {
        endpoint = requireUrl(value, "endpoint", false);
        issuer = requireUrl(value, "issuer", true);
		if (!sameOrigin(new URI(endpoint), new URI(issuer))) throw new IllegalArgumentException("endpoint must use the Auth issuer origin.");
        clientId = requireText(value, "clientId");
        namespace = requireText(value, "namespace");
        databaseName = value.optString("databaseName", "absolutejs-sync-local-v1");
		if (!databaseName.matches("[A-Za-z0-9._-]{1,128}")) throw new IllegalArgumentException("databaseName is invalid.");
        intervalMinutes = bounded(value.optInt("intervalMinutes", 15), 15, 1440, "intervalMinutes");
        maxMutations = bounded(value.optInt("maxMutations", 50), 0, 100, "maxMutations");
        maxPulls = bounded(value.optInt("maxPulls", 50), 0, 100, "maxPulls");
        maxAttempts = bounded(value.optInt("maxAttempts", 5), 1, 100, "maxAttempts");
    }
	private static boolean sameOrigin(URI left, URI right) {
		int leftPort = left.getPort() == -1 ? ("https".equals(left.getScheme()) ? 443 : 80) : left.getPort();
		int rightPort = right.getPort() == -1 ? ("https".equals(right.getScheme()) ? 443 : 80) : right.getPort();
		return left.getScheme().equals(right.getScheme()) && left.getHost().equalsIgnoreCase(right.getHost()) && leftPort == rightPort;
	}
    private static int bounded(int value, int min, int max, String name) {
        if (value < min || value > max) throw new IllegalArgumentException(name + " is out of range.");
        return value;
    }
    private static String requireText(JSONObject value, String name) throws JSONException {
        String text = value.getString(name).trim();
        if (text.isEmpty() || text.length() > 2048) throw new IllegalArgumentException(name + " is invalid.");
        return text;
    }
    private static String requireUrl(JSONObject value, String name, boolean originOnly) throws Exception {
        URI uri = new URI(requireText(value, name));
        boolean loopback = "http".equals(uri.getScheme()) && ("localhost".equals(uri.getHost()) || "127.0.0.1".equals(uri.getHost()) || "::1".equals(uri.getHost()));
        if (!"https".equals(uri.getScheme()) && !loopback) throw new IllegalArgumentException(name + " must use HTTPS.");
        if (uri.getUserInfo() != null || uri.getFragment() != null) throw new IllegalArgumentException(name + " is invalid.");
        if (originOnly && ((uri.getPath() != null && !uri.getPath().isEmpty() && !"/".equals(uri.getPath())) || uri.getQuery() != null)) throw new IllegalArgumentException(name + " must be an origin.");
        return uri.toString();
    }
    JSONObject json() throws JSONException {
        return new JSONObject().put("endpoint", endpoint).put("issuer", issuer).put("clientId", clientId).put("namespace", namespace).put("databaseName", databaseName).put("intervalMinutes", intervalMinutes).put("maxMutations", maxMutations).put("maxPulls", maxPulls).put("maxAttempts", maxAttempts);
    }
    static AbsoluteBackgroundSyncConfig load(Context context) throws Exception {
        String encoded = preferences(context).getString(CONFIG, null);
        return encoded == null ? null : new AbsoluteBackgroundSyncConfig(new JSONObject(encoded));
    }
    void save(Context context) throws JSONException { preferences(context).edit().putString(CONFIG, json().toString()).apply(); }
    static SharedPreferences preferences(Context context) { return context.getSharedPreferences(PREFERENCES, Context.MODE_PRIVATE); }
}
