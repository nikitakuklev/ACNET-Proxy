//  (c) 2010 Fermi Research Alliance
//  $Id: AbstractAcnetJob.java,v 1.4 2015/11/17 21:49:56 patrick Exp $


import gov.fnal.controls.daq.acquire.DaqJob;
import gov.fnal.controls.daq.events.DeviceDatabaseListObserver;
import gov.fnal.controls.daq.items.AcceleratorDevice;
import gov.fnal.controls.daq.items.AcceleratorDevicesItem;
import gov.fnal.controls.daq.items.DataItem;
import gov.fnal.controls.daq.oac.OpenAccessClientDataRepository;
import gov.fnal.controls.daq.util.AcnetException;
import gov.fnal.controls.service.dmq.JobException;
import gov.fnal.controls.service.dmq.impl.AbstractJob;
import gov.fnal.controls.service.dmq.impl.DAQStatus;
import gov.fnal.controls.service.proto.Dbnews;
import gov.fnal.controls.tools.drf2.DataRequest;
import gov.fnal.controls.tools.drf2.DiscreteRequest;
import gov.fnal.controls.tools.drf2.RequestFormatException;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Logger;

/**
 * Abstract implementation of ACNET Data Acquisition Job.
 * 
 * @author Andrey Petrov
 */
abstract class AbstractAcnetJob extends AbstractJob implements DeviceDatabaseListObserver {

    private static final boolean LOCAL = Boolean.getBoolean( "dmq.local-jobs" );
    protected final Set<DaqJob> daqJobs = new HashSet<DaqJob>();

    static boolean isLocal() {
        return LOCAL;
    }

    static DiscreteRequest parseRequest( String sreq ) throws JobException {
        try {
            DataRequest dreq = DataRequest.parse( sreq );
            if (!(dreq instanceof DiscreteRequest)) {
                throw new JobException(
                        DAQStatus.DMQ_FACILITY_CODE,
                        DAQStatus.INVALID_REQUEST,
                        "Invalid request type: " + sreq
                );
            }
            return (DiscreteRequest)dreq;
        } catch (RequestFormatException ex) {
            throw new JobException(
                    DAQStatus.DMQ_FACILITY_CODE,
                    DAQStatus.INVALID_REQUEST,
                    "Invalid data request: " + sreq
            );
        }
    }

    protected AbstractAcnetJob( Set<String> dataRequest ) throws JobException {
        super( dataRequest );
    }

    //
    // Subscribe to DbNews updates
    // If there are any changes to reading or setting scale factors
    // in jobs that are still active, then restart all DaqJobs in the request
    //
    public void deviceDatabaseListChange(Dbnews.Request.Info[] dbnews) {
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
    									Logger.getLogger(this.getClass().getName()).info("DbNews edit for device " + device.getName());
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
    
    protected abstract void restartJobs();

}
