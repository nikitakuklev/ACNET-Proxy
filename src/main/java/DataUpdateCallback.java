import gov.fnal.controls.service.dmq.TimedNumberCallback;
import gov.fnal.controls.tools.timed.TimedDoubleArray;
import gov.fnal.controls.tools.timed.TimedError;
import gov.fnal.controls.tools.timed.TimedNumber;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

class DataUpdateCallback extends TimedNumberCallback {
    private static final Logger logger = Logger.getLogger(DataUpdateCallback.class.getName());
    ConcurrentHashMap<String, TimedNumber> results = DRFCache.CACHE;
    ConcurrentHashMap<String, Integer> errors;
    HashSet<String> devices;
    int num_updates = 0;
    int num_errors = 0;
    int freq;
    long t1 = System.nanoTime();
    String name;

    public DataUpdateCallback(List<String> devices, String name, int freq) {
        this.devices = new HashSet<>(devices);
        this.errors = new ConcurrentHashMap<>(devices.size());
        devices.forEach(s -> this.errors.put(s,0));
        this.freq = freq;
        this.name = name;
    }

    public void dataChanged(String device, TimedNumber var2) {
        //System.out.println("Callback:" + var1 + "|" + var2);
        num_updates++;
        if (num_updates % this.freq == 0) {
            double rate = num_updates / ((System.nanoTime()-t1)/1e9);
            if (var2 instanceof TimedDoubleArray){
                System.out.println(String.format("%s | upd: %07d (%.2f /s) | err: %05d | %s %s",
                        this.name, num_updates, rate, num_errors, device,
                        Arrays.toString(Arrays.copyOfRange(((TimedDoubleArray) var2).doubleArray(), 0, 4))));
            } else {
                System.out.println(String.format("%s | upd: %07d (%.2f /s) | err: %05d | %s %s",
                        this.name, num_updates, rate, num_errors, device, var2));
            }
        }
        if (var2 instanceof TimedError) {
            //Note that the first received data sample will (most likely) be a <<72 1>> status message, indicating that the new request is pending.
            int error = ((TimedError) var2).getErrorNumber();
            int fc = ((TimedError) var2).getFacilityCode();
            if ((fc == 72) && (error == 1)) {
                return;
            }
            num_errors++;
            if (device != null) {
                if (errors.containsKey(device)) {
                    errors.put(device, errors.get(device) + 1);
                } else {
                    errors.put(device, 1);
                }
            } else {
                System.out.println("NULL Callback:" + var2);
                return;
            }

            if (Adapter3.bpms.contains(device)) {
                if ((fc == 57) && (error == -107)) {
                    return;
                } //MOOC_READ_TIMEOUT
                if ((fc == 57) && (error == -111)) {
                    return;
                } //MOOC_DRIVER_FAILED
                if ((fc == 68) && (error == 3)) {
                    return;
                } //BPM_DATA_UNAVAILABLE
            } else {
                if ((fc == 57) && (error == -89)) {
                    return;
                } //MOOC_BUSY
                if ((fc == 57) && (error == -107)) {
                    return;
                } //MOOC_READ_TIMEOUT
                if ((error == -89) || (error == -107) || (error == 1) || (error == 3)) {
                    // return; // These are common errors
                }
            }
            logger.warning(String.format("DataCB %s: weird error E:" + error + " | FC:" + fc + " | "+ var2 + " | " + device, this.name));
        }
        results.put(device, var2);
    }
}
