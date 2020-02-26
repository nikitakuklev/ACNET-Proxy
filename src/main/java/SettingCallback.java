import gov.fnal.controls.service.dmq.TimedNumberCallback;
import gov.fnal.controls.tools.timed.TimedNumber;

public class SettingCallback extends TimedNumberCallback {
    public SettingCallback() {

    }

    public void dataChanged(String drf, TimedNumber value) {
        System.out.println("\n\t>>> " + drf + "\t" + value);
    }
}

