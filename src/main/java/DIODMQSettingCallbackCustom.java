import gov.fnal.controls.service.dmq.TimedNumberCallback;
import gov.fnal.controls.tools.timed.TimedNumber;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class DIODMQSettingCallbackCustom extends TimedNumberCallback {
    long CUSTOM_SET_TIMEOUT = 8;
    SynchronousQueue<TimedNumber> returnQueue;
    TimedNumber returnData;

    DIODMQSettingCallbackCustom() {
        returnQueue = new SynchronousQueue<>();
    }

    @Override
    public void dataChanged( String dataRequest, TimedNumber data ) {
        try {
            returnQueue.put(data);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public TimedNumber getData() {
        try {
            long startSetWait = System.currentTimeMillis();
            TimedNumber result = returnQueue.poll(CUSTOM_SET_TIMEOUT, TimeUnit.SECONDS);
            long endSetWait = System.currentTimeMillis();
            long diff = endSetWait - startSetWait;
            if (result == null && diff < CUSTOM_SET_TIMEOUT * 1000) {
                System.out.println("DIODMQ.SettingCallback: null return after wait of only " + diff);
            }
            return result;
//			return returnQueue.take();
        } catch (InterruptedException ie) {
            System.out.println("DIODMQ.SettingCallback.getData wait interrupted: " + ie.getMessage());
            return null;
        }
    }

}