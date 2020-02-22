import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import gov.fnal.controls.service.proto.DAQData.Reply;
import gov.fnal.controls.tools.drf2.Event;
import gov.fnal.controls.webapps.getdaq.util.LogWriter;
import gov.fnal.controls.webapps.getdaq.web.errors.BadRequestException;
import gov.fnal.controls.webapps.getdaq.web.errors.InternalErrorException;
import gov.fnal.controls.webapps.getdaq.web.errors.ServiceUnavailableException;

/**
 * DAQ job cache manager. Should exist only in one instance.
 * @author Alexey Moroz
 * @version 1.0
 */
@SuppressWarnings({"unqualified-field-access","boxing"})
public class CacheManager {

    // Non-final variables used, providing a possibility to restart manager

    /**
     * Cache of DAQ jobs. Access must be synchronized.<br>
     *  <i>Cache-to-requests</i> mapping has <i>many-to-many</i> connection pattern.
     */
    private HashMap<DeviceEntity,CacheEntry> cache;

    /**
     * Full list of cache entries. Must be accessed in synchronized on cache block (synchronized(cache){...}).
     */
    private List<CacheEntry> listOfCacheEntries;

    /**
     * Launches DAQ jobs.
     */
    private ExecutorService executor; // extends java.util.concurrent.Executor interface

    /**
     * Application settings.
     */
    private Settings settings;

    /**
     * Garbage collector.
     */
    private Timer cacheCleaner;

    /**
     * Instance of cache manager.
     */
    private static CacheManager instance;

    /**
     * You are not supposed to use this. However, creation of multiple cache managers is still possible through Reflection.
     * @param settings application settings
     */
    private CacheManager(Settings settings){
        this.settings=settings;
        cache = new HashMap<DeviceEntity, CacheEntry>(settings.getMaximalNumberOfJobs()*settings.getNumberOfDevicesPerJob());
        listOfCacheEntries = new ArrayList<CacheEntry>(settings.getMaximalNumberOfJobs()/5+5); // ~20%
        executor = Executors.newFixedThreadPool(settings.getMaximalNumberOfJobs(),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r,Integer.toString(r.hashCode()));
                        t.setUncaughtExceptionHandler(new UncaughtExceptionHandler() { // anonymous inside anonymous
                            @Override
                            public void uncaughtException(@SuppressWarnings("hiding")Thread t, Throwable e) {
                                LogWriter.getInstance().write(
                                        "Pool thread #" +
                                                t.getName() +
                                                " terminated with exception " +
                                                e.getClass().getCanonicalName()+": "+e.getMessage()
                                );
                            }
                        });
                        return t;
                    }
                });
        cacheCleaner = new Timer(CacheCleaner.class.getName(), true);
        cacheCleaner.scheduleAtFixedRate(
                new CacheCleaner(),
                settings.getCacheCleaningInterval(),
                settings.getCacheCleaningInterval()
        );
    }

    /**
     * Returns reference to cache manager.
     * @param settings application settings
     * @return instance of cache manager
     */
    public static synchronized CacheManager getInstance(Settings settings){
        if(instance==null){
            instance=new CacheManager(settings);
        }
        return instance;
    }

    /**
     * Stops both data acquisition and cache manager. Further calls to any methods (except getInstance()) will throw NullPointerException.
     * @throws InterruptedException
     */
    public synchronized void stop(){
        synchronized(cache){
            Iterator<CacheEntry> iterator = listOfCacheEntries.iterator();
            while(iterator.hasNext()){
                executor.execute(iterator.next().getStopper());
            }
            listOfCacheEntries.clear();
            cache.clear();
        }
        cacheCleaner.cancel();
        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LogWriter.getInstance().write(e);
        }
        instance = null; // may call System.gc() after this
    }

    /**
     * Puts new DAQ Job into cache.
     * Cache entry will contain devices with only one event type.<br>
     * For example, M:OUTTMP.READING@U and M:OUTTMP.READING@P,2000,TRUE will be in different cache entries.
     * @param request Request object
     * @throws InternalErrorException if internal error occurs
     * @throws ServiceUnavailableException if application already handles too many jobs
     * @throws BadRequestException if too many devices were requested
     */
    public void submitRequest(Request request) throws BadRequestException, ServiceUnavailableException, InternalErrorException {
        submitDevices(request.getRequestedDevices());
    }

    /**
     * Puts new DAQ Job into cache.
     * Cache entry will contain devices with only one event type.<br>
     * For example, M:OUTTMP.READING@U and M:OUTTMP.READING@P,2000,TRUE will be in different cache entries.
     * @param devices devices to submit
     * @throws BadRequestException if too many devices were requested
     * @throws ServiceUnavailableException if application already handles too many jobs
     * @throws InternalErrorException if internal error occurs
     * @throws Exception if too many devices were requested
     */
    public void submitDevices(DeviceEntity[] devices) throws BadRequestException,
            ServiceUnavailableException,
            InternalErrorException {

        if(devices.length>settings.getNumberOfDevicesPerJob()){
            throw new BadRequestException("Too many devices requested");
        }

        // seek for different event types and create cache entry for each of them
        List<Event> events = new ArrayList<Event>(); // must have ordered array
        for(DeviceEntity device: devices){
            if(!events.contains(device.getDevice().getEvent())){
                events.add(device.getDevice().getEvent());
            }
        }
        CacheEntry[] entries = new CacheEntry[events.size()];
        for(int i = 0; i< entries.length; i++){
            entries[i] = new CacheEntry(settings);
        }

        synchronized(cache){ // this guarantees that only one thread writes to cache at any moment

            if((cache.size()==(settings.getMaximalNumberOfJobs()*settings.getNumberOfDevicesPerJob()))||
                    (listOfCacheEntries.size()==settings.getMaximalNumberOfJobs())){
                throw new ServiceUnavailableException("Too many Jobs");
            }

            try{
                for(DeviceEntity device: devices){ // separate devices by events
                    if(!cache.containsKey(device)){ // if such device is not present in cache
                        entries[events.indexOf(device.getDevice().getEvent())].addDevice(device);
                        cache.put(device, entries[events.indexOf(device.getDevice().getEvent())]);
                    }
                } // now entries contain devices separated by events
            }catch(IllegalArgumentException iae){
                throw new InternalErrorException(iae.getMessage());
            }

            for(CacheEntry entry: entries){
                if(entry.size()>0){
                    listOfCacheEntries.add(entry);
                    executor.execute(entry.getStarter());
                }
            }

        } //end of sync block

    }

    /**
     * Deletes requested devices from cache. Method restarts DAQ jobs which contained deleted devices.
     * @param request devices to delete
     */
    public void deleteDevices(Request request){
        deleteDevices(request.getRequestedDevices());
    }

    /**
     * Deletes requested devices from cache. Method restarts DAQ jobs which contained deleted devices.
     * @param devices devices to delete
     */
    public void deleteDevices(DeviceEntity[] devices) {

        CacheEntry[] modifiedEntries;
        List<CacheEntry> affectedEntries = new ArrayList<CacheEntry>(settings.getMaximalNumberOfJobs()/2); // List doubles number of its entries automatically
        List<DeviceEntity> devicesForDeletion = new ArrayList<DeviceEntity>(devices.length);

        synchronized (cache) { // seems to be an expensive and long operation

            for(DeviceEntity device: devices){
                devicesForDeletion.add(device);
                if(cache.containsKey(device)){
                    if(!affectedEntries.contains(cache.get(device))){
                        affectedEntries.add(cache.get(device));
                    }
                    listOfCacheEntries.remove(cache.get(device));
                    cache.remove(device);
                }
            }

            // Now 'affectedEntries' collection contains all jobs subjected to modification,
            // cache & listOfCacheEntries are cleared of affected cache entries
            modifiedEntries = new CacheEntry[affectedEntries.size()];

            for(int i=0; i<modifiedEntries.length; i++){

                modifiedEntries[i] = new CacheEntry(settings);
                DeviceEntity[] existingDevices = affectedEntries.get(i).getDevices();
                for(DeviceEntity deviceEntity: existingDevices){
                    if(!devicesForDeletion.contains(deviceEntity)){ // do not add devices marked for deletion
                        modifiedEntries[i].addDevice(deviceEntity);
                        cache.put(deviceEntity, modifiedEntries[i]);
                    }
                }
                executor.execute(affectedEntries.get(i).getStopper()); // stop jobs in deleted cache entries
                if(modifiedEntries[i].size()>0){
                    listOfCacheEntries.add(modifiedEntries[i]);
                    executor.execute(modifiedEntries[i].getStarter()); // launch modified job
                }

            } // modified entries launched

        } // end of synchronized block

        return; // after this there will be no references to previous cache entries, one may call System.gc()

    }

    /**
     * Returns replies for requested devices.<br>
     * Order of elements is saved due to List&lt;G&gt; classes used.
     * @param request request
     * @throws InternalErrorException if internal error occurs
     */
    public Reply[] getReplies(Request request) throws InternalErrorException {
        DeviceEntity[] devices=request.getRequestedDevices();
        List<Reply> list = new LinkedList<Reply>();
        synchronized(cache){
            for(int i = 0; i< devices.length; i++){
                Reply reply;
                reply = cache.get(devices[i]).getReply(devices[i]);
                if(reply!=null){
                    list.add(reply);
                }
            }
        }
        return list.toArray(new Reply[0]);
    }

    /**
     * Returns all entries contained in cache.
     * @return cache entries
     */
    public CacheEntry[] getEntries(){
        CacheEntry[] returnValue;
        synchronized(cache){
            returnValue=listOfCacheEntries.toArray(new CacheEntry[0]);
        }
        return returnValue;
    }

    /**
     * This class works as a daemon and cleans cache according to the settings specified.<br>
     * Checks if the DAQ job is either dead or not accessed, then removes it from cache.
     * Job is considered non-accessed when there were no requests for the devices contained during time period specified in the settings.
     * Called on timer.
     * @author Alexey Moroz
     * @version 1.0
     */
    @SuppressWarnings("synthetic-access") // yes, I know about overhead
    private final class CacheCleaner extends TimerTask {

        /**
         * This class works as a daemon and cleans cache according to the settings specified.<br>
         * Checks if the DAQ job is either dead or not accessed, then removes it from cache.
         * Job is considered non-accessed when there were no requests for the devices contained during time period specified in the settings.
         * Called on timer.
         * @author Alexey Moroz
         * @version 1.0
         */
        public CacheCleaner(){}

        @Override
        public void run() {
            try{
                synchronized(cache){ // accesses cache exclusively
                    Iterator<CacheEntry> iterator = listOfCacheEntries.iterator();
                    while(iterator.hasNext()){
                        CacheEntry entry = iterator.next();
                        long currentTime = System.currentTimeMillis();
                        long accessTime = entry.getLastAccessTime();
                        long allowedLifeTime = settings.getJobLivingTime();
                        boolean dead = true;
                        try {
                            dead=entry.isDead();
                        } catch (InternalErrorException e1) {
                            LogWriter.getInstance().write(e1);
                        }
                        if(dead | ((currentTime-accessTime)>=allowedLifeTime)){
                            executor.execute(entry.getStopper());
                            iterator.remove();
                            Iterator<Entry<DeviceEntity, CacheEntry>> iter = cache.entrySet().iterator();
                            while(iter.hasNext()){
                                Entry<DeviceEntity, CacheEntry> e = iter.next();
                                if(e.getValue()==entry){
                                    iter.remove();
                                }
                            } // end operations with cache
                        } // end is-dead-or-marked-for-deletion checking
                    } // end of iterations over job list
                } // end of synchronized block
            }catch(RuntimeException re){
                LogWriter.getInstance().write(re);
            }
        }

    }

}