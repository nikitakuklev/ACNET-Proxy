import gov.fnal.controls.service.dmq.TimedNumberCallback;
import gov.fnal.controls.tools.timed.TimedNumber;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class SettingCallback extends TimedNumberCallback {
    SynchronousQueue<TimedNumber> returnQueue = new SynchronousQueue<>();

    SettingCallback() {
    }

    public void dataChanged(String var1, TimedNumber var2) {
        try {
            this.returnQueue.put(var2);
        } catch (InterruptedException var4) {
            Thread.currentThread().interrupt();
        }

    }

    public TimedNumber getData() {
        try {
            long var1 = System.currentTimeMillis();
            TimedNumber var3 = this.returnQueue.poll(10L, TimeUnit.SECONDS);
            long var4 = System.currentTimeMillis();
            long var6 = var4 - var1;
            if (var3 == null && var6 < 10000L) {
                System.out.println("DIODMQ.SettingCallback: null return after wait of only " + var6);
            }

            return var3;
        } catch (InterruptedException var8) {
            System.out.println("DIODMQ.SettingCallback.getData wait interrupted: " + var8.getMessage());
            return null;
        }
    }
}
