import gov.fnal.controls.service.dmq.TimedNumberCallback;
import gov.fnal.controls.tools.timed.TimedError;
import gov.fnal.controls.tools.timed.TimedNumber;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class SettingCallback extends TimedNumberCallback {
    private static final Logger logger = Logger.getLogger(SettingCallback.class.getName());
    ConcurrentHashMap<String, LinkedBlockingDeque<TimedNumber>> lastValues;

    boolean acceptData;

    SettingCallback() {
        lastValues = new ConcurrentHashMap<>();
        acceptData = true;
    }

    @Override
    public void dataChanged(String dataRequest, TimedNumber data) {
        // Note the first message received will be the "pending" message.
        // This is dealt with in the main method
        // meh..check here too
        logger.info(String.format("Setting callback response (%s) = (%s)%n", dataRequest, data));
        if (data instanceof TimedError) {
            int error = ((TimedError) data).getErrorNumber();
            int fc = ((TimedError) data).getFacilityCode();
            if ((fc == 72) && (error == 1)) {
                logger.info("SettingCallback: got pending status 72-1, ignoring");
                return;
            }
        }
        try {
            LinkedBlockingDeque<TimedNumber> q = this.lastValues.get(dataRequest);
            if (q == null) {
                logger.severe(String.format("Could not find queue for %s%n", dataRequest));
                throw new NullPointerException();
            }
            if (acceptData) {
                boolean result = q.offer(data);
                if (!result) {
                    logger.severe(String.format("Queue of %s already had an item!%n", dataRequest));
                    q.clear();
                    q.offer(data);
                    //throw new IllegalArgumentException();
                }
            } else {
                logger.warning(String.format("SCB data (%s) arrived when not accepting", dataRequest));
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.warning(String.format("SCB data (%s) exception", dataRequest));
        }
    }

//    public TimedNumber getData() {
//        try {
//            long startSetWait = System.currentTimeMillis();
//            TimedNumber result = this.returnQueue.poll(DIODMQ.TIMEOUT, TimeUnit.SECONDS);
//            long endSetWait = System.currentTimeMillis();
//            long diff = endSetWait - startSetWait;
//            if (result == null && diff < DIODMQ.TIMEOUT * 1000) {
//                System.out.println("DIODMQ.SettingCallback: null return after wait of only " + diff);
//            }
//            return result;
////            if (result instanceof TimedError) {
////                return (TimedError) result;
////            } else {
////                return null;
////            }
////			return returnQueue.take();
//        } catch (InterruptedException ie) {
//            System.out.println("DIODMQ.SettingCallback.getData wait interrupted: " + ie.getMessage());
//            return null;
//        }
//    }

    public TimedNumber getDataAsync(String req, long start_poll_time) {
        try {
            long startSetWait = System.currentTimeMillis();
            long timeElapsed = startSetWait - start_poll_time;
            long timeWait = 10;
            if (timeElapsed <= 4200) {
                timeWait = 4200-timeElapsed;
            }
            TimedNumber result = this.lastValues.get(req).poll(timeWait, TimeUnit.MILLISECONDS);
            long endSetWait = System.currentTimeMillis();
            long diff = endSetWait - startSetWait;
            if (result == null && diff < 10) {
                System.out.println("SettingCB: null return after wait of only " + diff);
            }
            return result;
//            if (result instanceof TimedError) {
//                return (TimedError) result;
//            } else {
//                return null;
//            }
//			return returnQueue.take();
        } catch (InterruptedException ie) {
            System.out.println("SettingCB: wait interrupted: " + ie.getMessage());
            return null;
        }
    }

    public void clearData(String req) {
        if (!this.lastValues.containsKey(req)) {
            this.lastValues.put(req, new LinkedBlockingDeque<>(1));
        }
        this.lastValues.get(req).clear();
    }
}
