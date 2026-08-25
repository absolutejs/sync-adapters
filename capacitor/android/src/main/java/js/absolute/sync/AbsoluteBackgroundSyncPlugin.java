package js.absolute.sync;

import androidx.work.Constraints;
import androidx.work.ExistingPeriodicWorkPolicy;
import androidx.work.NetworkType;
import androidx.work.PeriodicWorkRequest;
import androidx.work.WorkManager;
import com.getcapacitor.Plugin;
import com.getcapacitor.PluginCall;
import com.getcapacitor.PluginMethod;
import com.getcapacitor.annotation.CapacitorPlugin;
import java.util.concurrent.TimeUnit;

@CapacitorPlugin(name = "AbsoluteBackgroundSync")
public class AbsoluteBackgroundSyncPlugin extends Plugin {
    private Constraints networkConstraint() { return new Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build(); }
    private void schedule(AbsoluteBackgroundSyncConfig config) {
        PeriodicWorkRequest request = new PeriodicWorkRequest.Builder(AbsoluteBackgroundSyncWorker.class, config.intervalMinutes, TimeUnit.MINUTES).setConstraints(networkConstraint()).build();
        WorkManager.getInstance(getContext()).enqueueUniquePeriodicWork(AbsoluteBackgroundSyncConfig.WORK_NAME, ExistingPeriodicWorkPolicy.UPDATE, request);
    }
    @PluginMethod public void configure(PluginCall call) {
        try {
            AbsoluteBackgroundSyncConfig config = new AbsoluteBackgroundSyncConfig(call.getData());
            config.save(getContext());
            schedule(config);
            call.resolve();
        } catch (Exception error) { call.reject("Invalid background Sync configuration.", "INVALID_ARGUMENT", error); }
    }
    @PluginMethod public void clear(PluginCall call) {
        WorkManager.getInstance(getContext()).cancelUniqueWork(AbsoluteBackgroundSyncConfig.WORK_NAME);
        AbsoluteBackgroundSyncConfig.preferences(getContext()).edit().clear().apply();
        call.resolve();
    }
    @PluginMethod public void runNow(PluginCall call) {
        new Thread(() -> {
            try {
                AbsoluteBackgroundSyncConfig config = AbsoluteBackgroundSyncConfig.load(getContext());
                if (config != null) AbsoluteBackgroundSyncEngine.run(getContext(), config);
                call.resolve(AbsoluteBackgroundSyncEngine.status(getContext()));
            } catch (AbsoluteBackgroundSyncEngine.NoSession error) {
                call.resolve(AbsoluteBackgroundSyncEngine.status(getContext()));
            } catch (Exception error) {
                AbsoluteBackgroundSyncEngine.recordError(getContext(), error);
                call.reject("Background Sync failed.", "SYNC_FAILURE", error);
            }
        }, "absolutejs-background-sync-manual").start();
    }
    @PluginMethod public void status(PluginCall call) { call.resolve(AbsoluteBackgroundSyncEngine.status(getContext())); }
}
