import com.fasterxml.jackson.databind.ObjectMapper;
import gov.fnal.controls.service.acnet.AcnetConnection;
import gov.fnal.controls.service.dpm.DPMDataHandler;
import gov.fnal.controls.service.dpm.DPMList;
import gov.fnal.controls.service.proto.DPM;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DPMConnManager {
    final DPMList[] lists;
    private static final Logger logger = Logger.getLogger(DPMConnManager.class.getName());

    public DPMConnManager() throws UnknownHostException {
        lists = new DPMList[]{makeList()};
    }

    public static void add_new(String request) {

    }

    public Result read(String request) throws Exception {
        DPMList list = makeList();
        OneShotTimer handler = new OneShotTimer();
        handler.reqTime = System.currentTimeMillis();
        list.addRequest(0, request);
        list.start(handler);
        while (handler.result == null) {
            Thread.sleep(10);
            if (System.currentTimeMillis() - handler.reqTime > 1000000) {
                //list.removeRequest(0);
                logger.warning("Read request timed out");
                break;
            }
        }
        list.close();
        return new Result(handler.result, handler.di);
    }

    public Result set(String request, double v) throws Exception {
        DPMList list = makeList();
        OneShotTimer handler = new OneShotTimer();
        handler.reqTime = System.currentTimeMillis();
        list.enableSettings("testing");
        list.addRequest(0, request);
        list.addSetting(0, v);
        list.applySettings(handler);
        //list.start(handler);
        while (handler.result == null) {
            Thread.sleep(10);
            if (System.currentTimeMillis() - handler.reqTime > 1000000) {
                //list.removeRequest(0);
                logger.warning("Read request timed out");
                break;
            }
        }
        list.close();
        return new Result(handler.result, handler.di);
    }

    private DPMList makeList() throws UnknownHostException {
        return DPMList.open(AcnetConnection.openProxy(), "MCAST");
    }
}

class Result {
    // Adapter to DMQ-style single result object
    final Object value;
    final DPM.Reply.DeviceInfo devInfo;

    public Result(Object val, DPM.Reply.DeviceInfo di) {
        value = val;
        devInfo = di;
    }

    public double doubleValue() {
        return (double) value;
    }

    public String getUnit() {
        return devInfo.units;
    }


}

class OneShotTimer implements DPMDataHandler {
    private static final Logger logger = Logger.getLogger(OneShotTimer.class.getName());
    long reqTime;
    Object result = null;
    DPM.Reply.DeviceInfo di = null;

    String timeDiff() {
        return "[" + (System.currentTimeMillis() - reqTime) + " ms]";
    }

    public void handle(DPM.Reply.DeviceInfo devInfo, DPM.Reply.Status s) {
        if (devInfo != null)
            logger.info(String.format("%d: status: %s = %04x @ %s %s\n", s.ref_id, devInfo.name, s.status, new Date(s.timestamp), timeDiff()));
        else
            logger.info(String.format("%d: status %04x @ %s %s\n", s.ref_id, s.status, new Date(s.timestamp), timeDiff()));
        result = s.status;
        di = devInfo;
    }

    public void handle(DPM.Reply.DeviceInfo devInfo, DPM.Reply.Scalar s) {
        logger.info(String.format("%d: %-8s = %.4f %s @ %s collection:%d cycle:%d (%g) %s\n",
                s.ref_id, devInfo.name, s.data, devInfo.units,
                new Date(s.timestamp), s.timestamp, s.cycle, s.data, timeDiff()));
        result = s.data;
        di = devInfo;
    }

    public void handle(DPM.Reply.DeviceInfo[] devInfo, DPM.Reply.ApplySettings m) {
        for (DPM.SettingStatus s : m.status)
            logger.info("ApplySettings Reply - id: " + s.ref_id + " status:" + s.status);
        result = m;
        di = null;
    }

}