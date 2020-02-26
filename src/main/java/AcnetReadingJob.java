//  (c) 2010 Fermi Research Alliance
//  $Id: AcnetReadingJob.java,v 1.17 2015/11/17 21:49:56 patrick Exp $

import static gov.fnal.controls.service.dmq.impl.DAQStatus.DMQ_FACILITY_CODE;
import static gov.fnal.controls.service.dmq.impl.DAQStatus.INVALID_REQUEST;
import static gov.fnal.controls.service.dmq.impl.DAQStatus.SERVER_ERROR;
import gov.fnal.controls.daq.acquire.DaqJob;
import gov.fnal.controls.daq.callback.DaqDataCallback;
import gov.fnal.controls.daq.datasource.AcceleratorSource;
import gov.fnal.controls.daq.datasource.DataSource;
import gov.fnal.controls.daq.datasource.MonitorChangeDisposition;
import gov.fnal.controls.daq.drf2.DataCallback;
import gov.fnal.controls.daq.drf2.DataCallbackFactory;
import gov.fnal.controls.daq.drf2.DeviceFactory;
import gov.fnal.controls.daq.drf2.EventFactory;
import gov.fnal.controls.daq.events.DataEvent;
import gov.fnal.controls.daq.events.DeviceDatabaseListObserver;
import gov.fnal.controls.daq.events.MulticastNews;
import gov.fnal.controls.daq.items.AcceleratorDevice;
import gov.fnal.controls.daq.items.AcceleratorDevicesItem;
import gov.fnal.controls.daq.items.DataItem;
import gov.fnal.controls.daq.oac.OpenAccessClientDataRepository;
import gov.fnal.controls.daq.util.AcnetException;
import gov.fnal.controls.service.dmq.JobException;
import gov.fnal.controls.service.dmq.ReadingJob;
import gov.fnal.controls.service.proto.DAQData.Reply;
import gov.fnal.controls.service.proto.Dbnews;
import gov.fnal.controls.tools.drf2.DiscreteRequest;
import gov.fnal.controls.tools.drf2.Event;
import gov.fnal.controls.tools.drf2.Field;
import gov.fnal.controls.tools.drf2.PeriodicEvent;
import gov.fnal.controls.tools.drf2.Property;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of ACNET reading job.
 *
 * @author Andrey Petrov
 */
class AcnetReadingJob extends AbstractAcnetJob implements ReadingJob, DeviceDatabaseListObserver {
    
    private static final Logger log = Logger.getLogger( AcnetReadingJob.class.getName());

    private final SmartDaqUser daqUser;
 //   private final Set<DaqJob> daqJobs = new HashSet<DaqJob>();
    private final Set<String> lookups = new HashSet<String>();
    private final StringBuilder requestString;
  
    private DeviceLookupService lookupService;

    AcnetReadingJob( Set<String> dataRequest, SmartDaqUser daqUser ) throws JobException {

        super( dataRequest );
        assert (daqUser != null);
        this.daqUser = daqUser;
        
        StringBuilder buf = createJobs(dataRequest, daqUser);
        requestString = buf;
        log.info( "Created " + this + ":" + buf );

        startJobs();
        startLookups();

        daqUser.incrementUseCount();
        
        MulticastNews.addDatabaseEditObserver(this);
    }
    
    private StringBuilder createJobs(Set<String> dataRequest, SmartDaqUser daqUser) throws JobException {

        // Event -> Set of ACNET devices
        Map<Event,Set<AcceleratorDevice>> consolidation =
                new HashMap<Event,Set<AcceleratorDevice>>();
        
        lookups.clear();
        daqJobs.clear();

        for (String sreq : dataRequest) {
            DiscreteRequest dreq = parseRequest( sreq );
            if (dreq.getProperty() == Property.DESCRIPTION ||
            	dreq.getProperty() == Property.INDEX ||
            	dreq.getProperty() == Property.LONG_NAME ||
            	dreq.getProperty() == Property.ALARM_LIST_NAME ) {
                lookups.add( sreq );
                continue;
            }

            // Create a separate job for each READING RAW request
            // These will use a separate dispostion that transmits the raw data directly
            // to the client rather than having the client receiving then unscaling the data
            if (dreq.getField() == Field.RAW && 
            		((dreq.getProperty() == Property.READING) || (dreq.getProperty() == Property.SETTING))) {
            	DaqJob job = createRawJob( sreq, dreq );
            	daqJobs.add( job );
            	continue;
            }
            
            Event event = dreq.getEvent();
            Set<AcceleratorDevice> devices = consolidation.get( event );
            if (devices == null) {
                devices = new HashSet<AcceleratorDevice>();
                consolidation.put( event, devices );
            }
            AcceleratorDevice device = createDevice( sreq, dreq );
            devices.add( device );
        }
        for (Map.Entry<Event,Set<AcceleratorDevice>> z : consolidation.entrySet()) {
            Event evt = z.getKey();
            Set<AcceleratorDevice> devs = z.getValue();
            DaqJob job = createJob( devs, evt );
            daqJobs.add( job );
        }

        if (daqJobs.isEmpty() && lookups.isEmpty()) {
            throw new JobException( DMQ_FACILITY_CODE, INVALID_REQUEST, "Empty request" );
        }

        StringBuilder buf = new StringBuilder();
        for (Map.Entry<Event,Set<AcceleratorDevice>> z : consolidation.entrySet()) {
            Event evt = z.getKey();
            Set<AcceleratorDevice> devs = z.getValue();
            buf.append( "\n    " );
            buf.append( createJobInfo( devs, evt ));
        }
        if (!lookups.isEmpty()) {
            buf.append( "\n    " );
            buf.append( createLookupInfo( lookups ));
        }
        return buf;
    }

    private AcceleratorDevice createDevice( String sreq, DiscreteRequest dreq )
            throws JobException {
        try {
        	// Rely on disposition callback for RAW READINGs
        	if (dreq.getField() == Field.RAW && dreq.getProperty() == Property.READING) {
        		return DeviceFactory.createReadingDevice( dreq, null );	
        	} else {
                DataCallback callback = new ReadingCallback( sreq );
                DaqDataCallback delegate = DataCallbackFactory.createDataCallback( dreq, callback );
            	return DeviceFactory.createReadingDevice( dreq, delegate );
            }
        } catch (IllegalArgumentException ex) {
            throw new JobException( DMQ_FACILITY_CODE, INVALID_REQUEST, ex.getMessage());
        }
    }
    
    private DataSource createSource() {
        return new AcceleratorSource();
    }

    private DataSource createDisposition( boolean onlyChanges ) {
        return new MonitorChangeDisposition( onlyChanges );
    }
    
    private DataSource createRawDisposition( String sreq, DiscreteRequest dreq, boolean onlyChanges ) {
        DataCallback callback = new ReadingCallback( sreq );
        DaqDataCallback delegate = DataCallbackFactory.createDataCallback( dreq, callback );
        return new DMQRawDisposition( delegate, onlyChanges );
    }

    private DataItem createItem( Collection<AcceleratorDevice> devs ) {
        AcceleratorDevicesItem item = new AcceleratorDevicesItem();
        for (AcceleratorDevice dev : devs) {
            item.addDevice( dev );
        }
        return item;
    }

    private DataEvent createEvent( Event evt ) throws JobException {
        try {
            return EventFactory.createEvent( evt );
        } catch (IllegalArgumentException ex) {
            throw new JobException( DMQ_FACILITY_CODE, INVALID_REQUEST, ex.getMessage());
        }
    }

    private boolean needsOnlyChanges( Event evt ) {
        return (evt instanceof PeriodicEvent) && !((PeriodicEvent)evt).inContinuous();
    }
    
    private DaqJob createJob( Collection<AcceleratorDevice> devs, Event evt ) throws JobException {
        DaqJob job = new DaqJob(
                createSource(),
                createDisposition( needsOnlyChanges( evt )),
                createItem( devs ),
                createEvent( evt ), 
                daqUser.getUser(),
                null 
        );
        if (isLocal()) {
            job.setLocalJob();
        }
        return job;
    }

    private DaqJob createRawJob( String sreq, DiscreteRequest dreq ) throws JobException {
        AcceleratorDevice device = createDevice( sreq, dreq );
        AcceleratorDevicesItem item = new AcceleratorDevicesItem();
        item.addDevice( device );
        
        DaqJob job = new DaqJob(
                createSource(),
                createRawDisposition( sreq, dreq, needsOnlyChanges( dreq.getEvent() )),
                item,
                createEvent( dreq.getEvent() ), 
                daqUser.getUser(),
                null 
        );
        if (isLocal()) {
            job.setLocalJob();
        }
        return job;
    }
    
    private String createJobInfo( Collection<AcceleratorDevice> devs, Event evt ) {
        StringBuilder buf = new StringBuilder();
        appendJobInfo( buf, devs, evt );
        return buf.toString();
    }

    private void appendJobInfo( StringBuilder buf, Collection<AcceleratorDevice> devs, Event evt ) {
        buf.append( '(' );
        boolean first = true;
        for (AcceleratorDevice dev : devs) {
            if (first) {
                first = false;
            } else {
                buf.append( ',' );
            }
            appendDeviceInfo( buf, dev );
        }
        buf.append( ')' );
        buf.append( '@' );
        buf.append( evt.toString());
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

    private String createLookupInfo( Collection<String> lookups ) {
        StringBuilder buf = new StringBuilder();
        buf.append( '(' );
        boolean first = true;
        for (String sreq : lookups) {
            if (first) {
                first = false;
            } else {
                buf.append( ',' );
            }
            buf.append( sreq );
        }
        buf.append( ')' );
        return buf.toString();
    }

    private void startJobs() throws JobException {
        try {
            int cnt = startJobsImpl();
            if (cnt > 0) {
                log.fine( "Started " + cnt + " DAQ reading job" + ((cnt == 1) ? "" : "s"));
            }
        } catch (Exception ex) {
            stopJobsImpl();
            log.log( Level.WARNING, "Cannot start DAQ reading job", ex );
            throw new JobException( DMQ_FACILITY_CODE, SERVER_ERROR, ex.getMessage());
        }
    }

    private int startJobsImpl() throws Exception {
        int cnt = 0;
        for (DaqJob job : daqJobs) {
            job.start();
            ++cnt;
        }
        return cnt;
    }

    private void stopJobs() {
        int cnt = stopJobsImpl();
        if (cnt > 0) {
            log.fine( "Stopped " + cnt + " DAQ reading job" + ((cnt == 1) ? "" : "s"));
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
//
// Stop then start jobs
// Don't restart lookup jobs
// Also it is possible but unlikely there could be one-shot jobs
// mixed with repetitive jobs, the one shot jobs will be redone
//
    	stopJobs();
    	try {
    		StringBuilder buf = createJobs(dataRequest, daqUser);
    		log.info("\nRestarting jobs " + buf);
    		startJobs();
    	} catch (JobException ex) {
    		log.severe("Exception restarting jobs " + ex);
    	}
    }

    private void startLookups() throws JobException {
        try {
            int cnt = startLookupsImpl();
            if (cnt > 0) {
                log.fine( "Initiated " + cnt + " device lookup" + ((cnt == 1) ? "" : "s"));
            }
        } catch (Exception ex) {
            log.log( Level.WARNING, "Cannot initiate device lookups", ex );
            throw new JobException( DMQ_FACILITY_CODE, SERVER_ERROR, ex.getMessage());
        }
    }

    private int startLookupsImpl() throws Exception {
        int cnt = 0;
        if (lookups.isEmpty()) {
            return cnt;
        }
        if (lookupService == null) {
            lookupService = new DeviceLookupService();
        }
        for (String sreq : lookups) {
            lookupService.lookup( sreq, this, true );
            ++cnt;
        }
        return cnt;
    }
    
    @Override
    public void addDataCallback( gov.fnal.controls.service.dmq.DataCallback callback ) {
        if (callback == null) {
            throw new NullPointerException();
        }
        callbacks.add( callback );
        if(lookupService != null) {
        	lookupService.flush();
        }
    }
    
    @Override
    public synchronized void dispose() {
        if (isDisposed()) {
            return;
        }
        daqUser.decrementUseCount();
        stopJobs();
        if (lookupService != null) {
            lookupService.dispose();
            lookupService = null;
        }
        super.dispose();
        log.info( "Disposed " + this + requestString.toString() );
    }

    @Override
    public String toString() {
        try {
            return "AcnetReadingJob@" + hashCode()
                    + "[user=" + daqUser.getUser().getUserName()
                    + ";host=" + daqUser.getUser().getUserNode() + "]";
        } catch (RemoteException ex) {
            return "AcnetReadingJob@" + hashCode();
        }
    }

    private class ReadingCallback implements DataCallback {

        private final String dataRequest;

        ReadingCallback( String dataRequest ) {
            this.dataRequest = dataRequest;
        }

        @Override
        public void dataChanged( Reply data ) {
            fireDataChanged( dataRequest, data );
        }

    }
    
    //
    // Subscribe to DbNews updates
    // If there are any changes to reading or setting scale factors
    // in jobs that are still active, then restart all DaqJobs in the request
    //
    public void deviceDatabaseListChangeX(Dbnews.Request.Info[] dbnews) {
    	long mask = OpenAccessClientDataRepository.DBNEWS_FLAG_READ_SCALING_MOD |
    			   OpenAccessClientDataRepository.DBNEWS_FLAG_SET_SCALING_MOD;
    	boolean restart = false;
    	for (DaqJob job : daqJobs) {
    		try {
    			if (job.stillExists()) {
    				DataItem item = job.getItem();
    				if (item instanceof AcceleratorDevicesItem) {
    					AcceleratorDevicesItem accItem = (AcceleratorDevicesItem) item;
    					Vector<Serializable> devices = accItem.getDevices();
    					for (Object obj : devices) {
    						if (obj instanceof AcceleratorDevice) {
    							AcceleratorDevice device = (AcceleratorDevice) obj;
    							int di = device.getDeviceIndex();
    							for (int ii = 0; ii < dbnews.length; ii++) {
    								if ((di == dbnews[ii].di) && (dbnews[ii].flags & mask) != 0) {
    									restart = true;
    									log.info("DbNews edit for device " + device.getName());
    								}
    							}   							
    						}
    					}   					
    				}
    			}
    		} catch (AcnetException ae) {
    		}
    	}

    	if (restart) {
    		new Thread(new Runnable() {
    			public void run() {
    				restartJobs();
    			}
    		}).start();
    	}
    }
}
