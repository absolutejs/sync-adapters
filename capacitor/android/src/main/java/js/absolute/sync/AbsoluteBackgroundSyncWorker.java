package js.absolute.sync;

import android.content.Context;
import androidx.annotation.NonNull;
import androidx.work.Worker;
import androidx.work.WorkerParameters;

public final class AbsoluteBackgroundSyncWorker extends Worker {
    public AbsoluteBackgroundSyncWorker(@NonNull Context context, @NonNull WorkerParameters parameters) { super(context, parameters); }
    @NonNull @Override public Result doWork() {
        try {
            AbsoluteBackgroundSyncConfig config = AbsoluteBackgroundSyncConfig.load(getApplicationContext());
            if (config == null) return Result.success();
            AbsoluteBackgroundSyncEngine.run(getApplicationContext(), config);
            return Result.success();
        } catch (AbsoluteBackgroundSyncEngine.NoSession error) {
            return Result.success();
        } catch (Exception error) {
            AbsoluteBackgroundSyncEngine.recordError(getApplicationContext(), error);
            return Result.retry();
        }
    }
}
