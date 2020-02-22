import gov.fnal.controls.daq.acquire.DaqJob;
import gov.fnal.controls.daq.acquire.DaqJobControl;
import gov.fnal.controls.daq.datasource.AcceleratorSource;
import gov.fnal.controls.daq.datasource.DataSource;
import gov.fnal.controls.daq.datasource.DelegatingCallbackDisposition;
import gov.fnal.controls.daq.drf2.DataCallbackFactory;
import gov.fnal.controls.daq.drf2.DeviceFactory;
import gov.fnal.controls.daq.drf2.EventFactory;
import gov.fnal.controls.daq.events.DataEvent;
import gov.fnal.controls.daq.items.AcceleratorDevicesItem;
import gov.fnal.controls.daq.util.AcnetException;
import gov.fnal.controls.service.proto.DAQData;
import gov.fnal.controls.service.proto.DAQData.Reply;
import gov.fnal.controls.webapps.getdaq.util.LogWriter;
import gov.fnal.controls.webapps.getdaq.web.errors.InternalErrorException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Class represents a separate DAQ job in cache.
 * @author Alexey Moroz
 * @version 1.0
 */
@SuppressWarnings({"unqualified-field-access", "boxing" })
public class CacheEntry {

    /**
     * Application settings.
     */
    private final Settings settings;

    /**
     * DAQ job.
     */
    private DaqJob job;

    /**
     * Internal list of devices.
     */
    private final List<DeviceEntity> requestedDevices;

    /**
     * Job status. True when data acquisition has been started, false otherwise.
     */
    private volatile boolean active;

    /**
     * Entry's last access time.
     */
    private volatile long lastAccessTime;

    /**
     * Entry's creation time.
     */
    private final long startTime;

    /**
     * Data acquisition starter.
     */
    private final DAQJobStarter starter;

    /**
     * Data acquisition stopper.
     */
    private final DAQJobStopper stopper;

    /**
     * Default constructor.
     * @param settings application settings
     */
    public CacheEntry(Settings settings){
        this.settings = settings;
        requestedDevices=new ArrayList<DeviceEntity>(settings.getNumberOfDevicesPerJob());
        starter = new DAQJobStarter();
        stopper = new DAQJobStopper();
        startTime=System.currentTimeMillis();
    }

    /**
     * Extended constructor.
     * @param settings application settings
     * @param devices devices to be added to internal list
     */
    public CacheEntry(Settings settings, DeviceEntity[] devices){
        this(settings);
        for(DeviceEntity device: devices){
            addDevice(device);
        }
    }

    /**
     * Create DAQ job and start data acquisition.
     * @throws IllegalStateException if DAQ had been already started
     * @throws InternalErrorException if job launching fails
     */
    private synchronized void startAcquisition() throws InternalErrorException {

        if(active){
            throw new IllegalStateException("CachEntry: Data acquisition has been already started");
        }

        // Creation of the new DAQ user is very expensive operation, thus outsourced to settings.
        // DaqUser user = new DaqUser(settings.getUserName(), settings.getHostName()); // create user

        DataSource from = new AcceleratorSource();
        DaqJobControl control = new DaqJobControl();
        DataEvent event = EventFactory.createEvent(
                requestedDevices.get(0).getDevice().getEvent()
        );
        DelegatingCallbackDisposition to = new DelegatingCallbackDisposition();

        AcceleratorDevicesItem item = new AcceleratorDevicesItem(); // create list of devices
        Iterator<DeviceEntity> iterator = requestedDevices.iterator();
        while(iterator.hasNext()){
            DeviceEntity device = iterator.next();
            boolean allowed=false;
            switch(device.getDevice().getProperty()){
                case READING:
                case SETTING:
                case ANALOG:
                case DIGITAL:
                case STATUS: allowed=true; break;
                case CONTROL:
                case DESCRIPTION: allowed=false; break;
                default: allowed = false; break;
            }
            if(allowed){
                try{
                    item.addDevice(
                            DeviceFactory.createReadingDevice(
                                    device.getDevice(),
                                    DataCallbackFactory.createDataCallback(
                                            device.getDevice(),
                                            device.getCallback()
                                    )
                            )
                    );
                }catch(Exception e){
                    LogWriter.getInstance().write(e); // java.lang.IllegalArgumentException: Array range not supported: T:baigsd.prdabl[:7]@e,2a
                }
            }
        }
        job = new DaqJob(from, to, item, event, settings.getUser(), control); // settings.getUser() does expensive operation once in lifetime

        try {
            job.start();
            job.waitForSetup();
        } catch (Exception e) {
            throw new InternalErrorException("CacheEntry: Could not start DAQ job: "+ e.getClass().getCanonicalName()+ ": " + e.getMessage());
        }finally{
            lastAccessTime=System.currentTimeMillis();
            active=true;
        }

    }

    /**
     * Stop DAQ.
     * @throws IllegalStateException job was already stopped or not yet started.
     * @throws InternalErrorException when job termination fails
     */
    private synchronized void stopAcquisition() throws InternalErrorException {

        if(!active){
            throw new IllegalStateException("CacheEntry: Data acquisition has not been started yet");
        }

        try {
            job.stop();
        } catch (Exception e) {
            throw new InternalErrorException("CacheEntry: Could not stop DAQ job: "+ e.getClass().getCanonicalName()+ ": " + e.getMessage());
        } finally {
            active = false;
        }

    }

    /**
     * Returns a Reply objects corresponding to this entry's devices.<br>
     * ATTENTION: reply can be null and likely is null immediately after DAQ job start. <br>
     * NOTE: is synchronized, for you not to be able to add a new device during reply retrieval.
     * @return devices' replies
     * @throws InternalErrorException if internal error occurs
     */
    public synchronized DAQData.Reply[] getReplies() throws InternalErrorException {
        List<Reply> replies = new LinkedList<Reply>();
        Iterator<DeviceEntity> iterator = this.requestedDevices.iterator();
        while(iterator.hasNext()){
            replies.add(getReply(iterator.next()));
        }
        return replies.toArray(new Reply[0]);
    }

    /**
     * Returns a Reply objects corresponding to this devices.<br>
     * ATTENTION: reply can be null and likely is null immediately after DAQ job start. <br>
     * NOTE: is synchronized, for you not to be able to add a new device during reply retrieval.
     * @param devices devices requested
     * @return corresponding replies
     * @throws InternalErrorException if internal error occurs
     */
    public synchronized Reply[] getReplies(DeviceEntity[] devices) throws InternalErrorException {
        List<Reply> replies = new LinkedList<Reply>();
        for(DeviceEntity device: devices){
            replies.add(getReply(device));
        }
        return replies.toArray(new Reply[0]);
    }

    /**
     * Returns a Reply object corresponding to this device.<br>
     * ATTENTION: reply can be null and likely is null immediately after DAQ job start. <br>
     * NOTE: is synchronized, for you not to be able to add a new device during reply retrieval.
     * @param device device requested
     * @return corresponding reply
     * @throws InternalErrorException if internal error occurs
     */
    public synchronized Reply getReply(DeviceEntity device) throws InternalErrorException {
        try{
            return requestedDevices.get(requestedDevices.indexOf(device)).getReply();
        }catch(Exception e){
            throw new InternalErrorException("CacheEntry: Could not get reply: "+e.getClass().getCanonicalName()+": "+e.getMessage());
        }finally{
            lastAccessTime = System.currentTimeMillis();
        }
    }

    /**
     * Adds device to internal array.<br>
     * Note, that you must restart DAQ if it was already started and you want to receive data from added device.
     * @param device device to be added
     */
    public synchronized void addDevice(DeviceEntity device){
        Iterator<DeviceEntity> iterator = requestedDevices.iterator();
        while(iterator.hasNext()){
            DeviceEntity entry = iterator.next();
            if(!device.getDevice().getEvent().equals(entry.getDevice().getEvent())){
                throw new IllegalArgumentException("CacheEntry: devices must have the same event.");
            }
        }
        requestedDevices.add(device);
    }

    /**
     * Returns list of all devices in internal array.
     * @return list of devices
     */
    public synchronized DeviceEntity[] getDevices(){
        return this.requestedDevices.toArray(new DeviceEntity[0]);
    }

    /**
     * Returns number of devices in internal array.
     * @return number of devices
     */
    public synchronized int size(){
        return this.requestedDevices.toArray(new DeviceEntity[0]).length;
    }

    /**
     * Returns this entry's last access time.
     * @return last access time
     */
    public long getLastAccessTime(){
        return this.lastAccessTime;
    }

    /**
     * Returns this entry's start time.
     * @return start time
     */
    public long getStartTime(){
        return startTime;
    }

    /**
     * Checks if DAQ job still exists.
     * @return existence status
     * @throws InternalErrorException if ACNET system generated an exception
     */
    public synchronized boolean isDead() throws InternalErrorException {
        try {
            if(job!=null){
                return !job.stillExists();
            }else{
                return false; // not yet created, cannot be dead
            }
        } catch (AcnetException e) {
            throw new InternalErrorException("CacheEntry: Could not check DAQ job status: "+ e.getClass().getCanonicalName()+ ": " + e.getMessage());
        }
    }

    /**
     * Returns a Runnable starter to be used with pool executor.
     * @return starter
     */
    public DAQJobStarter getStarter() {
        return this.starter;
    }

    /**
     * Returns a Runnable stopper to be used with pool executor.
     * @return stopper
     */
    public DAQJobStopper getStopper() {
        return this.stopper;
    }

    /**
     * Indicates whether other object is an instance of CacheEntry class and has the same list of devices.
     * @return true if cache entries have the same list of devices
     */
    @Override
    public boolean equals(Object obj) {
        if(obj instanceof CacheEntry){
            DeviceEntity[] devices = ((CacheEntry) obj).getDevices();
            if(devices.length!=requestedDevices.size()){
                return false;
            }
            for(DeviceEntity device: devices){
                if(!requestedDevices.contains(device)){
                    return false;
                }
            }
            return true;
        }else{
            return false;
        }
    }

    /**
     * Hash code is equal to all hashes of internal devices XORed:<br>
     * <b>hash = device[0] ^ device[1] ^ ... ^ device[size()-1]</b><br>
     * Device (containing DiscreteRequest object) has its own hashCode() method.
     * @return hash code
     */
    @Override
    public int hashCode() {
        Iterator<DeviceEntity> iterator = requestedDevices.iterator();
        int hash = 0x0;
        while(iterator.hasNext()){
            hash^=iterator.next().hashCode();
        }
        return hash;
    }

    /**
     * Job starter.
     * @author Alexey Moroz
     * @version 1.0
     */
    private class DAQJobStarter implements Runnable {

        public DAQJobStarter(){}

        @SuppressWarnings("synthetic-access")
        @Override
        public void run() {
            try {
                startAcquisition();
            } catch (IllegalStateException e) {
                LogWriter.getInstance().write(e);
            } catch (InternalErrorException e) {
                LogWriter.getInstance().write(e);
            }
        }

    }

    /**
     * Job stopper.
     * @author Alexey Moroz
     * @version 1.0
     */
    private class DAQJobStopper implements Runnable {

        public DAQJobStopper(){}

        @SuppressWarnings("synthetic-access")
        @Override
        public void run() {
            try {
                stopAcquisition();
            } catch (IllegalStateException e) {
                LogWriter.getInstance().write(e);
            } catch (InternalErrorException e) {
                LogWriter.getInstance().write(e);
            }
        }

    }

}