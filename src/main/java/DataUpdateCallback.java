import gov.fnal.controls.service.dmq.TimedNumberCallback;
import gov.fnal.controls.tools.timed.TimedError;
import gov.fnal.controls.tools.timed.TimedNumber;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

class DataUpdateCallback extends TimedNumberCallback {

    ConcurrentHashMap<String, TimedNumber> results = DRFCache.CACHE;
    ConcurrentHashMap<String, Integer> errors;
    HashSet<String> devices;
    int num_updates = 0;
    int num_errors = 0;
    int freq;
    String name;

    public DataUpdateCallback(List<String> devices, String name, int freq) {
        this.devices = new HashSet<>(devices);
        this.errors = new ConcurrentHashMap<>(devices.size());
        devices.forEach(s -> this.errors.put(s,0));
        this.freq = freq;
        this.name = name;
    }

    public void dataChanged(String var1, TimedNumber var2) {
        //System.out.println("Callback:" + var1 + "|" + var2);
        num_updates++;
        if (num_updates % this.freq == 0) {
            System.out.println(String.format("%s | updates: %07d | errors: %05d | %s",
                    this.name, num_updates, num_errors, var2));
        }
        if (var2 instanceof TimedError) {
            num_errors++;
            if (var1 != null) {
                errors.put(var1, errors.get(var1) + 1);
            }
            else {
                System.out.println("NULL Callback:" + var2);
                return;
            }
            int error = ((TimedError) var2).getErrorNumber();
            int fc = ((TimedError) var2).getFacilityCode();
            if (Adapter2.bpms.contains(var1)) {
                if ((fc == 57) && (error == -107)) {
                    return;
                } //MOOC_READ_TIMEOUT
                if ((fc == 57) && (error == -111)) {
                    return;
                } //MOOC_DRIVER_FAILED
                if ((fc == 68) && (error == 3)) {
                    return;
                } //BPM_DATA_UNAVAILABLE
            }
            if ((error == -89) || (error == -107) || (error == 1) || (error == 3)) {
               // return; // These are common errors
            }
            System.out.println("Weird error: E:" + error + " | FC:" + fc + " | "+ var2 + " | " + var1);
        }
        results.put(var1, var2);
    }
}
