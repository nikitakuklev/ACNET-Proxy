//  (c) 2009 Fermi Research Alliance
//  $Id: DeviceLookupService.java,v 1.5 2015/08/20 19:36:43 patrick Exp $


import gov.fnal.controls.service.dmq.impl.AbstractJob;
import gov.fnal.controls.service.proto.DAQData.Reply;
import gov.fnal.controls.tools.drf2.DataRequest;
import gov.fnal.controls.tools.drf2.DiscreteRequest;
import gov.fnal.controls.tools.drf2.Property;
import gov.fnal.controls.tools.timed.TimedError;
import gov.fnal.controls.tools.timed.TimedInteger;
import gov.fnal.controls.tools.timed.TimedNumber;
import gov.fnal.controls.tools.timed.TimedNumberFactory;
import gov.fnal.controls.tools.timed.TimedString;

import java.util.Hashtable;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

/**
 * Service for looking up description of ACNET devices.
 *
 * @author Andrey Petrov
 */
class DeviceLookupService {
    
    private static final Logger log = Logger.getLogger( DeviceLookupService.class.getName());

    private static final String CONTEXT_FACTORY = "gov.fnal.controls.service.naming.ContextFactory";

    private static final int DAQ_FACILITY_CODE = 0;
    private static final int DAQ_ERROR_CODE = 0;

    private final ExecutorService cron = Executors.newSingleThreadExecutor();
    private final Vector<LookupTask> taskWait = new Vector<LookupTask>(20);
    private final Context initContext;

    DeviceLookupService() throws NamingException {
        Hashtable<String,Object> env = new Hashtable<String,Object>();
        env.put( Context.INITIAL_CONTEXT_FACTORY, CONTEXT_FACTORY );
        initContext = new InitialDirContext( env );
        log.info( "Device lookup created" );
    }
    
    public void lookup( String sreq, AbstractJob job ) {
    	lookup( sreq, job, false );
    }

    // If wait is true, the tasks are created and stored in a Vector
    // for later execution by calling the flush() method.
    // There is an issue that if there are many requests then the data
    // starts coming back before the callback is created, hence it is lost
    
    public void lookup( String sreq, AbstractJob job, boolean wait ) {
        if (sreq == null || job == null) {
            throw new NullPointerException();
        }
        DiscreteRequest dreq;
        try {
            DataRequest req = DataRequest.parse( sreq );
            if (!(req instanceof DiscreteRequest)) {
                log.fine( "Not a discrete request" );
                return;
            }
            dreq = (DiscreteRequest)req;
        } catch (Exception ex) {
            log.throwing( DeviceLookupService.class.getName(), "lookup", ex );
            return;
        }
        LookupTask task = new LookupTask( dreq, sreq, job );
        if (wait) {
        	taskWait.add( task );
        } else {
        	cron.submit( task );
        }
    }
    
    public void flush() {
    	for (LookupTask task : taskWait) {
    		cron.submit(task);
    	}
    	taskWait.removeAllElements();
    }

    public void dispose() {
        try {
            initContext.close();
        } catch (Throwable ex) {
            log.throwing( getClass().getName(), "dispose", ex );
        }
        cron.shutdownNow();
        log.info( "Device lookup shut down" );
    }

    private class LookupTask implements Runnable {

        private final DiscreteRequest dreq;
        private final String sreq;
        private final AbstractJob job;

        private DirContext root;

        LookupTask( DiscreteRequest dreq, String sreq, AbstractJob job ) {
            this.dreq = dreq;
            this.sreq = sreq;
            this.job = job;
            try {
                root = (DirContext)initContext.lookup( "device" );
            } catch (NamingException ex) {
                log.log( Level.WARNING, "Device DB unavailable", ex );
            }
        }

        @Override
        public void run() {
            TimedNumber num = lookupDevice( dreq.getDevice());
            Reply proto = TimedNumberFactory.toProto( num );
            job.fireDataChanged( sreq, proto );
        }

        TimedNumber lookupDevice( String deviceName ) {
            if (root == null) {
                return new TimedError(
                        DAQ_FACILITY_CODE,
                        DAQ_ERROR_CODE,
                        "Device DB unavailable",
                        System.currentTimeMillis()
                );
            }
            try {
            	Property prop = this.dreq.getProperty();
            	if (prop == Property.INDEX) {
            		Integer value = (Integer)root.getAttributes( deviceName ).get( "INDEX" ).get();
            		return new TimedInteger(value);
            	} else {
            		String value = (String)root.getAttributes( deviceName ).get( prop.toString() ).get();
            		return new TimedString( value );
            	}
            } catch (NameNotFoundException ex) {
                return new TimedError(
                        DAQ_FACILITY_CODE,
                        DAQ_ERROR_CODE,
                        "Device not found: " + deviceName,
                        System.currentTimeMillis()
                );
            } catch (NamingException ex) {
                return new TimedError(
                        DAQ_FACILITY_CODE,
                        DAQ_ERROR_CODE,
                        ex.getMessage(),
                        System.currentTimeMillis()
                );
            }
        }

    }

}
