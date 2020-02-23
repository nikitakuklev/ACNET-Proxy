
import gov.fnal.controls.daq.acquire.DaqUser;
import gov.fnal.controls.webapps.getdaq.util.LogWriter;

/**
 * Class represents application settings.
 * @author Alexey Moroz
 * @version 1.0
 */
@SuppressWarnings({ "unqualified-field-access", "boxing" })
public final class Settings {

    /* CONSTANTS */

    /**
     * Name for 'username' init parameter in web.xml
     */
    private final static String usernameInitParamName="daeUser";

    /**
     * Name for 'host' init parameter in web.xml
     */
    private final static String hostnameInitParamName="daeHost";

    /**
     * Name for 'cacheCleaningInterval' init parameter in web.xml
     */
    private final static String cacheCleaningIntervalInitParamName="cacheCleaningInterval";

    /**
     * Name for 'numberOfDevicesPerJob' init parameter in web.xml
     */
    private final static String numberOfDevicesPerJobInitParamName="numberOfDevicesPerJob";

    /**
     * Name for 'maximalNumberOfJobs' init parameter in web.xml
     */
    private final static String maximalNumberOfJobsInitParamName="maximalNumberOfJobs";

    /**
     * Name for 'jobLivingTime' init parameter in web.xml
     */
    private final static String jobLivingTimeInitParamName="jobLivingTime";

    /**
     * Default value for cacheCleaningInterval is 30 seconds.
     */
    private final static Long defaultCacheCleaningInterval=new Long(30000);

    /**
     * Default maximal number of devices per DAQ job is 128.
     */
    private final static Integer defaultNumberOfDevicesPerJob=128;

    /**
     * Default maximal number of DAQ jobs is 64.
     */
    private final static Integer defaultMaximalNumberOfJobs=64;

    /**
     * Default job living time is 5 minutes.
     */
    private final static Long defaultJobLivingTime=new Long(300000);

    /**
     * Default username is empty.
     */
    private final static String defaultUserName="";

    /**
     * Default hostname is localhost.
     */
    //private final static String defaultHostName="localhost";
    private final static String defaultHostName="dse01.fnal.gov";
    //private final static String defaultHostName="ctl040pc.fnal.gov";

    /* PRIVATE STUFF */

    /**
     * An interval in milliseconds between successive calls to cache cleaner.
     */
    private volatile Long cacheCleaningInterval;

    /**
     * The maximal number of devices per each cache entry.<br>
     * Client can ask for data from greater number of devices, but not more than <i><b>numberOfDevicesPerJob*maximalNumberOfJobs</b></i>.
     */
    private volatile Integer numberOfDevicesPerJob;

    /**
     * The maximal number of DAQ jobs which can be performed simultaneously.
     * If client's request requires application to exceed that number, HTTP Error 503 (Service Not Available) will be thrown.
     */
    private volatile Integer maximalNumberOfJobs;

    /**
     * Time in milliseconds after which job will be stopped if not accessed.
     */
    private volatile Long jobLivingTime;

    /**
     * Username to be provided to DAE.
     */
    private volatile String userName;

    /**
     * Hostname to be provided to DAE.
     */
    private volatile String hostName;


    /* SINGLETON'S STUFF */

    /**
     * Settings object reference.
     */
    private static Settings instance;

    private final DaqUser user; // user creation is very expensive, thus we do it only once

    /**
     * Singleton constructor. Loads settings from web.xml file.
     */
    private Settings(){
        try {
            cacheCleaningInterval=defaultCacheCleaningInterval;
            numberOfDevicesPerJob=defaultNumberOfDevicesPerJob;
            maximalNumberOfJobs=defaultMaximalNumberOfJobs;
            jobLivingTime=defaultJobLivingTime;
            userName=defaultUserName;
            hostName=defaultHostName;
        } catch (Exception e) {
            loadDefaults();
            LogWriter.getInstance().write(e);
        }finally{
            user = new DaqUser(userName, hostName); // create user
        }
    }

    /**
     * Loads default application settings.
     */
    private void loadDefaults(){
        cacheCleaningInterval=defaultCacheCleaningInterval;
        numberOfDevicesPerJob=defaultNumberOfDevicesPerJob;
        maximalNumberOfJobs=defaultMaximalNumberOfJobs;
        jobLivingTime=defaultJobLivingTime;
        userName=defaultUserName;
        hostName=defaultHostName;
    }

    /**
     * Returns a reference to an initialized Settings instance.
     * @return Settings instance
     */
    public static synchronized Settings getInstance(){
        if(instance==null){
            instance=new Settings();
        }
        return instance;
    }

    /* METHODS */

    /**
     * Returns time interval in milliseconds between successive cache cleans.
     * @return cacheCleaningInterval
     */
    public Long getCacheCleaningInterval() {
        return cacheCleaningInterval;
    }

    /**
     * Returns number of devices per DAQ job.
     * @return numberOfDevicesPerJob
     */
    public Integer getNumberOfDevicesPerJob() {
        return numberOfDevicesPerJob;
    }

    /**
     * Returns maximal number of simultaneous DAQ jobs.
     * @return maximalNumberOfJobs
     */
    public Integer getMaximalNumberOfJobs() {
        return maximalNumberOfJobs;
    }

    /**
     * Returns maximum lifetime of an unattended job.
     * @return jobLivingTime
     */
    public Long getJobLivingTime() {
        return jobLivingTime;
    }

    /**
     * Returns username to be used by DAE.
     * @return username
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Returns hostname to be used by DAE.
     * @return hostname
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * Returns DAQ user object.
     * @return DAQ user
     */
    public DaqUser getUser(){
        return user;
    }

    /* END */

}