//  (c) 2010 Fermi Research Alliance
//  $Id: AcnetSettingJob.java,v 1.16 2015/11/17 21:49:56 patrick Exp $

import static gov.fnal.controls.service.dmq.impl.DAQStatus.DMQ_FACILITY_CODE;
import static gov.fnal.controls.service.dmq.impl.DAQStatus.INVALID_DATA_TYPE;
import static gov.fnal.controls.service.dmq.impl.DAQStatus.INVALID_REQUEST;
import static gov.fnal.controls.service.dmq.impl.DAQStatus.SERVER_ERROR;
import static gov.fnal.controls.service.dmq.impl.DAQStatus.SETTING_DISABLED;
import gov.fnal.controls.daq.acquire.DaqJob;
import gov.fnal.controls.daq.acquire.SettingsState;
import gov.fnal.controls.daq.acquire.SettingsStateException;
import gov.fnal.controls.daq.callback.DaqDataCallback;
import gov.fnal.controls.daq.callback.SettingCompleteCallback;
import gov.fnal.controls.daq.datasource.AcceleratorDisposition;
import gov.fnal.controls.daq.datasource.AcceleratorSource;
import gov.fnal.controls.daq.datasource.DataSource;
import gov.fnal.controls.daq.datasource.MonitorChangeDisposition;
import gov.fnal.controls.daq.datasource.UserSettingSource;
import gov.fnal.controls.daq.drf2.DataCallback;
import gov.fnal.controls.daq.drf2.DataCallbackFactory;
import gov.fnal.controls.daq.drf2.DataSetter;
import gov.fnal.controls.daq.drf2.DeviceFactory;
import gov.fnal.controls.daq.drf2.SetterFactory;
import gov.fnal.controls.daq.events.DataEvent;
import gov.fnal.controls.daq.events.DefaultDataEvent;
import gov.fnal.controls.daq.events.DeviceDatabaseListObserver;
import gov.fnal.controls.daq.events.KnobSettingEvent;
import gov.fnal.controls.daq.events.MulticastNews;
import gov.fnal.controls.daq.items.AcceleratorDevice;
import gov.fnal.controls.daq.items.AcceleratorDevicesItem;
import gov.fnal.controls.daq.items.DataItem;
import gov.fnal.controls.daq.oac.OpenAccessClientDataRepository;
import gov.fnal.controls.daq.util.AcnetException;
import gov.fnal.controls.service.dmq.JobException;
import gov.fnal.controls.service.dmq.SettingJob;
import gov.fnal.controls.service.proto.DAQData.Reply;
import gov.fnal.controls.service.proto.Dbnews;
import gov.fnal.controls.tools.drf2.DiscreteRequest;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of ACNET setting job.
 *
 * @author Andrey Petrov
 */
class AcnetSettingJob extends AbstractAcnetJob implements SettingJob {

    private static final int SETTING_ENABLE_TIME = 10; // MINUTES
    
    private static final Logger log = Logger.getLogger( AcnetSettingJob.class.getName());

    private final Map<String,DataSetter> setters = new HashMap<String,DataSetter>();

    private final SmartDaqUser daqUser;
//    private final Set<DaqJob> daqJobs = new HashSet<DaqJob>();
    private final String requestString;
    
    private volatile AcceleratorDevicesItem settingItem;

    AcnetSettingJob( Set<String> dataRequest, SmartDaqUser daqUser ) throws JobException {

        super( dataRequest );
        
        assert (daqUser != null);
        this.daqUser = daqUser;
        
        String jobInfo = createJobs(dataRequest, daqUser);
        
        requestString = jobInfo;
        log.info( "Created " + this + ":\n    " + jobInfo );
        
        startJobs();

        daqUser.incrementUseCount();
        
        MulticastNews.addDatabaseEditObserver(this);
        
    }
    
    private String createJobs(Set<String> dataRequest, SmartDaqUser daqUser) throws JobException {

    	daqJobs.clear();
    	
        List<AcceleratorDevice> sDevs = new LinkedList<AcceleratorDevice>(); // setting devices
        List<AcceleratorDevice> rDevs = new LinkedList<AcceleratorDevice>(); // reading devices
        
        // NOTE: In order to set fields of analog and digital alarm properties, we have to read 
        // the corresponding alarm blocks first, then update the necessry field (keeping the
        // rest of the data structure unchanged), and submit the whole thing back to the
        // control system. Java DAQ framework doesn't support setting of an individual field.

        DaqDataCallback delegate = null;
        for (String sreq : dataRequest) {
            
            DiscreteRequest dreq = parseRequest( sreq );
            
            DataCallback callback = new SettingCallback( sreq );
            delegate = DataCallbackFactory.createDataCallback( dreq, callback );
            AcceleratorDevice sDev = createSettingDevice( dreq, delegate );
            sDevs.add( sDev );
            DataSetter setter = SetterFactory.createSetter( dreq, sDev );
            
            if (setter instanceof DaqDataCallback) {
                // In order to set, need to read current value first
                rDevs.add( createReadingDevice( dreq, (DaqDataCallback)setter ));
            }
            
            setters.put( sreq, setter );
            
        }
        
        if (sDevs.isEmpty()) {
            throw new JobException( DMQ_FACILITY_CODE, INVALID_REQUEST, "Empty request" );
        }
        DaqJob sJob = createSettingJob( sDevs, delegate );
        daqJobs.add( sJob );
        settingItem = (AcceleratorDevicesItem)sJob.getItem();
        
        if (!rDevs.isEmpty()) {
            daqJobs.add( createReadingJob( rDevs ));
        }

        enableSettings();
        if (!isSettingEnabled()) {
            throw new JobException( DMQ_FACILITY_CODE, SETTING_DISABLED, "Setting disabled" );
        }

        String jobInfo = createJobInfo( sDevs );
        return jobInfo;

    }

    private AcceleratorDevice createSettingDevice( DiscreteRequest dreq ) throws JobException {
        try {
            return DeviceFactory.createSettingDevice( dreq );
        } catch (IllegalArgumentException ex) {
            throw new JobException( DMQ_FACILITY_CODE, INVALID_REQUEST, ex.getMessage());
        }
    }
    
    private AcceleratorDevice createSettingDevice( DiscreteRequest dreq, DaqDataCallback callback ) throws JobException {
        try {
            return DeviceFactory.createSettingDevice( dreq, callback );
        } catch (IllegalArgumentException ex) {
            throw new JobException( DMQ_FACILITY_CODE, INVALID_REQUEST, ex.getMessage());
        }
    }
    
    private AcceleratorDevice createReadingDevice( DiscreteRequest dreq,  
            DaqDataCallback callback ) throws JobException {
        try {
            return DeviceFactory.createDevice( dreq, callback, false, false );
        } catch (IllegalArgumentException ex) {
            throw new JobException( DMQ_FACILITY_CODE, INVALID_REQUEST, ex.getMessage());
        }
    }
    
    private DataSource createSettingSource( DaqDataCallback callbackDelegate ) {
        return new UserSettingSource((SettingCompleteCallback) callbackDelegate );
    }

    private DataSource createSettingDisposition() {
        return new AcceleratorDisposition();
    }

    private DataItem createSettingItem( Collection<AcceleratorDevice> devs ) {
        AcceleratorDevicesItem item = new AcceleratorDevicesItem();
        for (AcceleratorDevice dev : devs) {
            item.addDevice( dev );
        }
        return item;
    }

    private DataEvent createSettingEvent() {
        return new KnobSettingEvent();
    }

    private DaqJob createSettingJob( Collection<AcceleratorDevice> devs, DaqDataCallback delegate ) throws JobException {
        DaqJob job = new DaqJob(
                createSettingSource( delegate ),
                createSettingDisposition(),
                createSettingItem( devs ),
                createSettingEvent(),
                daqUser.getUser(),
                null 
        );
        if (isLocal()) {
            job.setLocalJob();
        }
        return job;
    }
    
    private DataSource createReadingSource() {
        return new AcceleratorSource();
    }

    private DataSource createReadingDisposition() {
        return new MonitorChangeDisposition( true ); // true = only changes
    }

    private DataItem createReadingItem( Collection<AcceleratorDevice> devs ) {
        AcceleratorDevicesItem item = new AcceleratorDevicesItem();
        for (AcceleratorDevice dev : devs) {
            item.addDevice( dev );
        }
        return item;
    }

    private DataEvent createReadingEvent() throws IllegalArgumentException {
        return new DefaultDataEvent();
    }
    
    private DaqJob createReadingJob( Collection<AcceleratorDevice> devs ) throws JobException {
        return new DaqJob(
            createReadingSource(),
            createReadingDisposition(),
            createReadingItem( devs ),
            createReadingEvent(), 
            daqUser.getUser(),
            null 
        );
    }

    private String createJobInfo( Collection<AcceleratorDevice> devs ) {
        StringBuilder buf = new StringBuilder();
        appendJobInfo( buf, devs );
        return buf.toString();
    }

    private void appendJobInfo( StringBuilder buf, Collection<AcceleratorDevice> devs ) {
        boolean first = true;
        for (AcceleratorDevice dev : devs) {
            if (first) {
                first = false;
            } else {
                buf.append( ',' );
            }
            appendDeviceInfo( buf, dev );
        }
    }

    private void appendDeviceInfo( StringBuilder buf, AcceleratorDevice dev ) {
        try {
            String devInfo = DeviceFactory.getRequest( dev ).toString();
            buf.append( devInfo );
        } catch (Exception ex) {
            log.throwing( getClass().getName(), "appendDeviceInfo", ex );
            buf.append( '?' );
        }
    }
    
    
    private void startJobs() throws JobException {
        try {
            int cnt = startJobsImpl();
            if (cnt > 0) {
                log.fine( "Started " + cnt + " DAQ setting job" + ((cnt == 1) ? "" : "s"));
            }
        } catch (Exception ex) {
            stopJobsImpl();
            log.log( Level.WARNING, "Cannot start DAQ setting job", ex );
            throw new JobException( DMQ_FACILITY_CODE, SERVER_ERROR, ex.getMessage());
        }
    }

    private int startJobsImpl() throws Exception {
        int cnt = 0;
        for (DaqJob job : daqJobs) {
            job.start();
            job.waitForSetup();
            ++cnt;
        }
        return cnt;
    }
    
    private void stopJobs() {
        int cnt = stopJobsImpl();
        if (cnt > 0) {
            log.fine( "Stopped " + cnt + " DAQ setting job" + ((cnt == 1) ? "" : "s"));
        }
    }

    private int stopJobsImpl() {
        int cnt = 0;
        for (DaqJob job : daqJobs) {
            try {
                if (job.stillExists()) {
                    job.stop();
                    job.waitForCompletion();
                    ++cnt;
                }
            } catch (Throwable ex) {
                log.throwing( getClass().getName(), "stopJobs", ex );
            } finally {
                job.clearReferences();
            }
        }
        return cnt;
    }
    
    @Override
    protected void restartJobs() {
    	stopJobs();
    	try {
    		String buf = createJobs(dataRequest, daqUser);
    		log.info("\nRestarting jobs " + buf);
    		startJobs();
    	} catch (JobException ex) {
    		log.severe("Exception restarting jobs " + ex);
    	}
    }
    
    @Override
    public boolean isSettingEnabled() {
        SettingsState ss = daqUser.getUser().getWho().getCurrentSettingsState();
        return ss.isSettingsAllowed();
    }

    private void enableSettings() throws JobException {
        try {
            SettingsState ss = new SettingsState( true, SETTING_ENABLE_TIME );
            daqUser.getUser().getWho().modifySettingsState( ss );
        } catch (SettingsStateException ex) {
            throw new JobException( DMQ_FACILITY_CODE, SETTING_DISABLED, "Cannot enable setting: " + ex.getMessage());
        }
    }

    @Override
    public synchronized void setData( String sreq, Reply sample ) throws JobException {

    	if (sreq == null || sample == null) {
            throw new NullPointerException();
        }
        DataSetter setter = setters.get( sreq );
        if (setter == null) {
            throw new JobException( DMQ_FACILITY_CODE, INVALID_REQUEST, "Invalid request; setData DataSetter is null" );
        }
        if (!isSettingEnabled()) {
            enableSettings();
            log.fine( "Setting re-enabled in the running job" );
        }
        try {
            setter.setData(sample);
            if (!setter.settingReady()) {
                setter.settingReady( true );
            }
            settingItem.processSettings();
            setter.settingReady( false );

        } catch (IllegalArgumentException ex) {
            throw new JobException( DMQ_FACILITY_CODE, INVALID_DATA_TYPE, "Invalid data type " + ex.getMessage() +
            				"\n Job: " + this.toString() + ";request=" + requestString);
        } catch (Exception ex) {
            throw new JobException( DMQ_FACILITY_CODE, SERVER_ERROR, ex.getMessage() +
            				"\n Job: " + this.toString() + ";request=" + requestString);
        }
    }

    @Override
    public synchronized void dispose() {
        if (isDisposed()) {
            return;
        }
        daqUser.decrementUseCount();
        stopJobs();
        super.dispose();
        log.info( "Disposed " + this + "\n" + requestString );
    }

    @Override
    public String toString() {
        try {
            return "AcnetSettingJob@" + hashCode()
                    + "[user=" + daqUser.getUser().getUserName()
                    + ";host=" + daqUser.getUser().getUserNode() + "]";
        } catch (RemoteException ex) {
            return "AcnetSettingJob@" + hashCode();
        }
    }

    private class SettingCallback implements DataCallback {

    	private final String dataRequest;

    	SettingCallback( String dataRequest ) {
    		this.dataRequest = dataRequest;
    	}

    	@Override
    	public synchronized void dataChanged( Reply data ) {
    		// Send status back to ServerSettingJob
    		fireDataChanged( dataRequest, data );
    	}

    }
}