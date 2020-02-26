import gov.fnal.controls.daq.acquire.*;
import gov.fnal.controls.daq.datasource.AcceleratorDisposition;
import gov.fnal.controls.daq.datasource.AcceleratorSource;
import gov.fnal.controls.daq.datasource.DataSource;
import gov.fnal.controls.daq.datasource.DelegatingCallbackDisposition;
import gov.fnal.controls.daq.drf2.DataCallbackFactory;
import gov.fnal.controls.daq.drf2.DeviceFactory;
import gov.fnal.controls.daq.drf2.EventFactory;
import gov.fnal.controls.daq.events.DataEvent;
import gov.fnal.controls.daq.events.KnobSettingEvent;
import gov.fnal.controls.daq.items.AcceleratorDevice;
import gov.fnal.controls.daq.items.AcceleratorDevicesItem;
import gov.fnal.controls.daq.util.AcnetException;
import gov.fnal.controls.kerberos.KerberosLoginContext;
import gov.fnal.controls.service.dmq.JobException;
import gov.fnal.controls.service.dmq.SettingJob;
import gov.fnal.controls.service.dmq.impl.SubjectInfo;
import gov.fnal.controls.service.dmq.impl.UserInfo;
import gov.fnal.controls.service.proto.DAQData;
import gov.fnal.controls.tools.timed.TimedDouble;
import gov.fnal.controls.tools.timed.TimedNumberFactory;
import gov.fnal.controls.webapps.getdaq.util.LogWriter;
import gov.fnal.controls.webapps.getdaq.web.errors.BadRequestException;
import gov.fnal.controls.webapps.getdaq.web.errors.InternalErrorException;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import java.rmi.RemoteException;
import java.security.Principal;
import java.util.*;

public class DaqJobTests {

    public static void main(String[] args) throws BadRequestException, InternalErrorException, InterruptedException, JobException, RemoteException, AcnetException, LoginException {
        KerberosLoginContext krb = KerberosLoginContext.getInstance();
        krb.login();
        System.out.println(krb.getSubject());
        Subject s = krb.getSubject();
        Principal princ = KerberosLoginContext.getPrincipal(s);
        System.out.println(princ.getName());
        //startAcquisition();
        //startSetting2();
        startSetting();


//        while (true) {
//            System.out.println(Arrays.toString(getReplies()));
//            Thread.sleep(5000);
//        }
    }

    private static final List<DeviceEntity> requestedDevices = new ArrayList<DeviceEntity>(64);;

    private static synchronized void startAcquisition() throws InternalErrorException, BadRequestException, InterruptedException {
        // Creation of the new DAQ user is very expensive operation, thus outsourced to settings.
        //String defaultHostName = "dse01.fnal.gov";
        //String defaultHostName="ctl040pc.fnal.gov";
        //String defaultHostName="localhost";
        //String defaultHostName = "dce47.fnal.gov";
        //String defaultHostName = "131.225.132.198";
        String defaultHostName = "due12.fnal.gov";
        //String defaultHostName = "dce01.fnal.gov";
        //String defaultHostName = "belobog";
        String defaultUserName = "comrade";
        //String unparsedRequest = "M:OUTTMP@P,1000;N:IBPSTATD.STATUS@P,500";
        String unparsedRequest = "Z:ACLTST@P,1000";


        String[] requests = RequestParser.parseRequest(unparsedRequest, true);
        DeviceEntity[] devices = RequestParser.getDeviceEntities(requests);
        for (DeviceEntity device : devices) {
            System.out.println(device + "|" + device.getDevice().getEvent());
        }
        //requestedDevices = new ArrayList<DeviceEntity>(64);
        for (DeviceEntity device : devices) {
            requestedDevices.add(device);
        }
        DataSource from = new AcceleratorSource();
        DaqJobControl control = new DaqJobControl();
        DataEvent event = EventFactory.createEvent(requestedDevices.get(0).getDevice().getEvent());
        DelegatingCallbackDisposition to = new DelegatingCallbackDisposition();
        AcceleratorDevicesItem item = new AcceleratorDevicesItem(); // create list of devices
        for (DeviceEntity device : requestedDevices) {
            try {
                AcceleratorDevice ad = DeviceFactory.createReadingDevice(
                        device.getDevice(),
                        DataCallbackFactory.createDataCallback(
                                device.getDevice(),
                                device.getCallback()
                        )
                );
                item.addDevice(ad);
                dump(ad);
                System.out.println(ad);
            } catch (Exception e) {
                LogWriter.getInstance().write(e); // java.lang.IllegalArgumentException: Array range not supported: T:baigsd.prdabl[:7]@e,2a
            }
        }
        DaqJob.setClassBugs(true);
        DaqUser user = new DaqUser(defaultUserName, defaultHostName); // create user
        //DaqUser user = new DaqUser("test"); // create user
        DaqJob job = new DaqJob(from, to, item, event, user, control); // settings.getUser() does expensive operation once in lifetime
        try {
            job.start();
            System.out.println("started!");
            job.waitForSetup();
        } catch (Exception e) {
            throw new InternalErrorException("CacheEntry: Could not start DAQ job: " + e.getClass().getCanonicalName() + ": " + e.getMessage());
        }
    }

    private static synchronized void startSetting2() throws JobException {
        String defaultHostName = "dse01.fnal.gov";
        String defaultUserName = "nkuklev@fnal.gov";
        String unparsedRequest = "Z:ACLTST.SETTING";
        ArrayList<String> reqs = new ArrayList<String>(Collections.singleton(unparsedRequest));
        HashSet<String> hs = new HashSet<>(reqs);


        DaqUser user = new DaqUser(defaultUserName, defaultHostName);
        SmartDaqUser suser = new SmartDaqUser(user);
        AcnetSettingJob sj = new AcnetSettingJob(hs,suser);
//        Subject s = KerberosLoginContext.getInstance().getSubject();
//        System.out.println("Subject:" + s);
//        UserInfo ui = new SubjectInfo(s, "127.0.0.1", "test");
        //AcnetJobFactory f = new AcnetJobFactory();
        //SettingJob sj = f.createSettingJob(hs,ui);
        System.out.println(sj);
        TimedDouble td = new TimedDouble(6.5);
        DAQData.Reply reply = TimedNumberFactory.toProto(td);
        sj.setData(reqs.get(0),reply);
    }


    private static synchronized void startSetting() throws InternalErrorException, BadRequestException, InterruptedException, RemoteException, AcnetException {
        String defaultHostName = "due12.fnal.gov";
        String defaultUserName = "nkuklev@FNAL.GOV";
        double value = 1.5;
        String unparsedRequest = "Z:ACLTST.SETTING";
        String[] requests = RequestParser.parseRequest(unparsedRequest, true);
        DeviceEntity[] devices = RequestParser.getDeviceEntities(requests);
        for (DeviceEntity device : devices) {
            System.out.println(device + "|" + device.getDevice().getEvent());
            requestedDevices.add(device);
        }
        DataSource from = new AcceleratorSource();
        AcceleratorDisposition to = new AcceleratorDisposition();
        DaqJobControl control = new DaqJobControl();
        DataEvent event = new KnobSettingEvent();
        AcceleratorDevicesItem item = new AcceleratorDevicesItem(); // create list of devices
        for (DeviceEntity device : requestedDevices) {
            AcceleratorDevice ad = DeviceFactory.createReadingDevice(
                    device.getDevice(),
                    DataCallbackFactory.createDataCallback(
                            device.getDevice(),
                            device.getCallback()
                    )
            );
            item.addDevice(ad);
            dump(ad);
            System.out.println(ad);
        }
        DaqJob.setClassBugs(true);
        DaqUser user = new DaqUser(defaultUserName, defaultHostName); // create user
        //DaqUser user = new DaqUser("test"); // create user
        DaqJob job = new DaqJob(from, to, item, event, user, control); // settings.getUser() does expensive operation once in lifetime
        DaqWho dw = user.getWho();
        System.out.println(dw);
        System.out.println(dw.getAccountPrivileges());
        System.out.println(dw.getServicePrivileges());
        System.out.println(dw.getNodePrivileges());
        job.start();
        System.out.println("started!");
        job.waitForSetup();
        try {
            //SettingsState var1 = new SettingsState(true, 10);
            //user.getWho().modifySettingsState(var1);

        } catch (Exception e) {
            throw new InternalErrorException("CacheEntry: Could not start DAQ job: " + e.getClass().getCanonicalName() + ": " + e.getMessage());
        }
        while (true) {
            System.out.println(Arrays.toString(getReplies()));
            Thread.sleep(5000);
        }
    }

    private static void dump(AcceleratorDevice var0) {
        System.out.println("\nName:            " + var0.getName());
        System.out.println("Property Index:  " + var0.getPropertyIndex());
        System.out.println("Data Offset:     " + var0.getDataOffset());
        System.out.println("Data Length:     " + var0.getDataLength());
        System.out.println("Array Element:   " + var0.getArrayElement());
        System.out.println("Number Elements: " + var0.getNumberElements());
        System.out.println("Full Array:      " + var0.wantsAllArrayElements());
        System.out.println("Setting Flag:    " + var0.getSettingFlag());
    }

    public static synchronized DAQData.Reply[] getReplies() throws InternalErrorException {
        List<DAQData.Reply> replies = new LinkedList<DAQData.Reply>();
        Iterator<DeviceEntity> iterator = requestedDevices.iterator();
        while(iterator.hasNext()){
            replies.add(getReply(iterator.next()));
        }
        return replies.toArray(new DAQData.Reply[0]);
    }

    public static synchronized DAQData.Reply getReply(DeviceEntity device) throws InternalErrorException {
        try{
            return requestedDevices.get(requestedDevices.indexOf(device)).getReply();
        }catch(Exception e){
            throw new InternalErrorException("CacheEntry: Could not get reply: "+e.getClass().getCanonicalName()+": "+e.getMessage());
        }
    }
}
