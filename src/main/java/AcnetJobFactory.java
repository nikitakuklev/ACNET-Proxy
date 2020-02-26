//  (c) 2010 Fermi Research Alliance
//  $Id: AcnetJobFactory.java,v 1.7 2011/05/12 15:23:54 apetrov Exp $

import gov.fnal.controls.daq.acquire.DaqUser;
import gov.fnal.controls.daq.acquire.DaqWho;
import gov.fnal.controls.service.dmq.Disposable;
import gov.fnal.controls.service.dmq.JobException;
import gov.fnal.controls.service.dmq.ReadingJob;
import gov.fnal.controls.service.dmq.SettingJob;
import gov.fnal.controls.service.dmq.impl.DAQStatus;
import gov.fnal.controls.service.dmq.impl.DaemonThreadFactory;
import gov.fnal.controls.service.dmq.impl.JobFactory;
import gov.fnal.controls.service.dmq.impl.UserInfo;
import gov.fnal.controls.service.dmq.impl.acnet.AcnetJobFactoryMBean;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Factory for creation of ACNET jobs.
 *
 * @author Andrey Petrov
 */
class AcnetJobFactory implements JobFactory, AcnetJobFactoryMBean, Disposable {

    private static final String DEFAULT_APP_NAME = "DEFAULT_DMQ_APPLICATION";

    private static final Logger log = Logger.getLogger( AcnetJobFactory.class.getName());

    private final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    private final Map<UserInfo,SmartDaqUser> users = new HashMap<UserInfo,SmartDaqUser>();
    private final ScheduledExecutorService exec;

    private int purgeRate;
    private Future<?> purgeFuture;
    private boolean disposed;

    public AcnetJobFactory() {
        exec = Executors.newSingleThreadScheduledExecutor(
            new DaemonThreadFactory( this + " worker thread" )
        );
        setPurgeRate( Config.SERVER_PURGE_RATE );
        registerMBean();
        log.fine( "Created " + this );
    }

    @Override
    public int getPurgeRate() {
        return purgeRate;
    }

    @Override
    public final void setPurgeRate( int purgeRate ) {
        if (this.purgeRate == purgeRate) {
            return;
        }
        if (purgeFuture != null) {
            purgeFuture.cancel( true );
            purgeFuture = null;
        }
        this.purgeRate = purgeRate;
        if (purgeRate > 0) {
            purgeFuture = exec.scheduleAtFixedRate(
                    new PurgeTask(),
                    purgeRate,
                    purgeRate,
                    TimeUnit.MILLISECONDS
            );
        }
    }

    @Override
    public synchronized int getDaqUserCount() {
        return users.size();
    }

    @Override
    public ReadingJob createReadingJob(
            Set<String> dataRequest,
            UserInfo userInfo ) throws JobException {
        return new AcnetReadingJob( dataRequest, getDaqUser( userInfo ));
    }

    @Override
    public SettingJob createSettingJob(
            Set<String> dataRequest,
            UserInfo userInfo ) throws JobException {
        return new AcnetSettingJob( dataRequest, getDaqUser( userInfo ));
    }

    private synchronized SmartDaqUser getDaqUser( UserInfo userInfo ) throws JobException {
        UserInfo key = new ImmutableUserInfo( userInfo );
        SmartDaqUser daqUser = users.get( key );
        if (daqUser == null) {
            daqUser = createDaqUser( userInfo );
            users.put( key, daqUser );
            log.info( "Created DAQ user on behalf of " + key );
        }
        return daqUser;
    }

    private SmartDaqUser createDaqUser( UserInfo userInfo ) throws JobException {

        String user = userInfo.getUser();
        if (user == null) {
            throw new JobException(
                    DAQStatus.DMQ_FACILITY_CODE,
                    DAQStatus.SECURITY_VIOLATION,
                    "User not specified"
            );
        }

        // Removing realm prefix from the user name
        /*
        int i = user.indexOf( '@' );
        if (i != -1) {
            user = user.substring( 0, i );
        }
        */

        String host = userInfo.getHost();
        if (host == null) {
            throw new JobException(
                    DAQStatus.DMQ_FACILITY_CODE,
                    DAQStatus.SECURITY_VIOLATION,
                    "Host not specified"
            );
        }

        String appName = userInfo.getAppName();
        if (appName == null) {
            appName = DEFAULT_APP_NAME;
        }

        try {
            DaqWho who = DaqWho.getDaqWho( appName );
            DaqUser daqUser = new DaqUser( user, "localhost$$" + host, who );
            return new SmartDaqUser( daqUser );
        } catch (Exception ex) {
            log.log( Level.WARNING, "Cannot create DAQ user", ex );
            throw new JobException(
                    DAQStatus.DMQ_FACILITY_CODE,
                    DAQStatus.SERVER_ERROR,
                    "Cannot create DAQ user: " + ex.getMessage()
            );
        }

    }

    @Override
    public synchronized int purge() {
        int n = 0;
        for (Iterator<SmartDaqUser> z = users.values().iterator(); z.hasNext(); ) {
            SmartDaqUser u = z.next();
            if (u.getUseCount() != 0) {
                continue;
            }
            z.remove();
            u.getUser().releaseResources();
            ++n;
        }
        if (n > 0) {
            log.fine( "Evicted " + n + " loitering DAQ user" + (n > 1 ? "s" : "" ));
        }
        return n;
    }

    private ObjectName getMBeanName() throws Exception {
        Hashtable<String,String> items = new Hashtable<String,String>();
        items.put( "type", getClass().getSimpleName());
        items.put( "name", String.valueOf( hashCode()));
        return new ObjectName( Config.MBEAN_DOMAIN, items );
    }

    private void registerMBean() {
        try {
            mbeanServer.registerMBean( this, getMBeanName());
        } catch (Exception ex) {
            log.throwing( AcnetJobFactory.class.getName(), "registerMBean", ex );
        }
    }

    private void unregisterMBean() {
        try {
            mbeanServer.unregisterMBean( getMBeanName());
        } catch (Exception ex) {
            log.throwing( AcnetJobFactory.class.getName(), "unregisterMBean", ex );
        }
    }

    @Override
    public boolean isDisposed() {
        return disposed;
    }

    @Override
    public void dispose() {
        if (isDisposed()) {
            return;
        }
        disposed = true;
        exec.shutdownNow();
        unregisterMBean();
        log.fine( "Disposed " + this );
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + hashCode();
    }

    private static class ImmutableUserInfo extends UserInfo {

        private final String user;

        ImmutableUserInfo( UserInfo userInfo ) {
            super( userInfo.getHost(), userInfo.getAppName());
            this.user = userInfo.getUser();
        }

        @Override
        public String getUser() {
            return user;
        }

        @Override
        public int hashCode() {
            return ((user != null) ? user.hashCode() : 0)
                    ^ ((host != null) ? host.hashCode() : 0)
                    ^ ((appName != null) ? appName.hashCode() : 0);
        }

        @Override
        public boolean equals( Object obj ) {
            return (obj instanceof ImmutableUserInfo)
                    && equals( ((ImmutableUserInfo)obj).user, user )
                    && equals( ((ImmutableUserInfo)obj).host, host )
                    && equals( ((ImmutableUserInfo)obj).appName, appName );
        }

        private static boolean equals( String s1, String s2 ) {
            return (s1 == null) ? s2 == null : s1.equals( s2 );
        }

    }

    private class PurgeTask implements Runnable {

        @Override
        public void run() {
            try {
                purge();
            } catch (Throwable ex) {
                log.log( Level.SEVERE, "Task execution failed", ex );
            }

        }

    }

}
