import gov.fnal.controls.service.dmq.DaqClient;
import gov.fnal.controls.service.dmq.JobException;
import gov.fnal.controls.service.dmq.SettingJob;
import gov.fnal.controls.service.proto.DAQData;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

public class DIODMQSettingJobCustom {
    private SettingJob job;
    private DIODMQSettingCallbackCustom settingCallback;
    public DaqClient cl;
    public Set<String> dataRequest;

    private static final Logger logger = Logger.getLogger(UndertowHandler.class.getName());

    DIODMQSettingJobCustom(DaqClient daqClient, String device) throws InterruptedException {
        String[] requestArray = new String[1];
        requestArray[0] = device;
        Set<String> dr = new HashSet<String>( Arrays.asList( requestArray ));

        cl = daqClient;
        dataRequest = dr;
        job = daqClient.createSettingJob(dr);
        settingCallback = new DIODMQSettingCallbackCustom();
        job.addDataCallback(settingCallback);
    }

    public SettingJob getSettingJob() {
        if (this.job.isDisposed()) {
            logger.warning(String.format("CUSTOM JOB [%s] LOST SETTING JOB", this.dataRequest));
            if (this.cl.isDisposed()) {
                logger.warning("CLIENT IS DISPOSED, THIS WONT WORK");
            }
            this.job = this.cl.createSettingJob(dataRequest);
            this.settingCallback = new DIODMQSettingCallbackCustom();
            this.job.addDataCallback(this.settingCallback);
        }
//        try {
//            Class<? extends AbstractJob> sjclass = (Class<? extends AbstractJob>) Class.forName("gov.fnal.controls.service.dmq.impl.rabbit.ClientSettingJob");
//            AbstractJob sjc = sjclass.cast(this.job);
//            sjc.
//        } catch (ClassNotFoundException e) {
//            throw new RuntimeException(e);
//        }
        return this.job;
    }

    public DIODMQSettingCallbackCustom getDataCallback() {
        return settingCallback;
    }

    public void setData(String request, DAQData.Reply data) throws JobException {
        job.setData(request, data);
    }
}
