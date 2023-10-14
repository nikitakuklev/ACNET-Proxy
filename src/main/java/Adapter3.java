import gov.fnal.controls.daq.acquire.SettingsState;
import gov.fnal.controls.kerberos.KerberosLoginContext;
import gov.fnal.controls.service.dmq.*;
//import gov.fnal.controls.service.dmq.impl2.probe.SettingApp;
import gov.fnal.controls.tools.dio.DIODMQ;
import gov.fnal.controls.tools.drf2.DataRequest;
import gov.fnal.controls.tools.drf2.DiscreteRequest;
import gov.fnal.controls.tools.logging.LogInit;
import gov.fnal.controls.tools.timed.TimedError;
import gov.fnal.controls.tools.timed.TimedNumber;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import org.xnio.Options;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;



public class Adapter3 {

    private static final Logger logger = Logger.getLogger(Adapter3.class.getName());
    public static DaqClient daqReadingClient = null;
    public static DaqClient daqSettingClient = null;

    public static LinkedList<DaqClient> daqSettingClients = null;
    public static DaqClient daqDefaultClient = null;
    //public static SettingJob settingJob = null;
    public static LinkedList<SettingJob> settingJobs = null;
    //public static SettingCallback settingCallback = null;
    public static ConcurrentHashMap<SettingJob, SettingCallback> settingCallbackMap = null;
    public static ConcurrentHashMap<String, DataUpdateCallback> callbacks = new ConcurrentHashMap<>();
    public static HashSet<String> bpms = null;
    public static HashSet<String> readers = null;
    public static HashSet<String> setters = null;
    public static LinkedList<HashSet<String>> settersList = new LinkedList<>();
    public static String preset;
    public static DefaultCredentialHandler credHandler = null;

    public static void main(String[] args) {
        //System.setProperty("dmq.heartbeat-rate", "1000");
        //System.setProperty("dmq.amqp-heartbeat-rate", "1000");
        System.setProperty("dmq.max-idle-time", "0"); //15000
        System.setProperty("dmq.client.init-rate", "2000"); //5000
        System.setProperty("dmq.client.lock-timeout", "4000"); //8000

        settingJobs = new LinkedList<>();
        settingCallbackMap = new ConcurrentHashMap<>();
        daqSettingClients = new LinkedList<>();

        if (args.length == 0) {
            preset = "rabbit-1";
        } else {
            preset = args[0];
        }

        LogInit.initializeLogs();
        logger.setUseParentHandlers(true);
        logger.log(Level.INFO, "ACNET-Proxy starting up with preset " + preset);
        logger.info("TEMP: " + System.getProperty("java.io.tmpdir"));
        logger.info("CWD: " + System.getProperty("user.dir"));
        //" %tY-%<tm-%<td %<tH:%<tM:%<tS.%<tL%<tz",
        //							logRecord.getMillis()
        try {
            Charset charset = Charset.defaultCharset();


//            BufferedReader is3 = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream("setting_devices.txt")));
//            List<String> s3set = is3.lines().map(s -> s.split("@")[0]).collect(Collectors.toList());
//            List<String> s3 = is3.lines().map(s -> s.split("@")[0] + "@p,1000").collect(Collectors.toList());
//            HashSet<String> setters_temp = new HashSet<>(s3set);
//            setters.addAll(setters_temp);
//            settersList.add(setters_temp);
//            logger.info(String.format("Setters list of length %d read", setters_temp.size()));

//            BufferedReader is4 = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream("setting_devices_2.txt")));
//            List<String> s4set = is4.lines().map(s -> s.split("@")[0]).collect(Collectors.toList());
//            List<String> s4 = is4.lines().map(s -> s.split("@")[0] + "@p,1000").collect(Collectors.toList());
//            setters_temp = new HashSet<>(s4set);
//            setters.addAll(setters_temp);
//            settersList.add(setters_temp);
//            logger.info(String.format("Setters list of length %d read", setters_temp.size()));
//
//            BufferedReader is5 = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream("setting_devices_3.txt")));
//            List<String> s5set = is5.lines().map(s -> s.split("@")[0]).collect(Collectors.toList());
//            List<String> s5 = is5.lines().map(s -> s.split("@")[0] + "@p,1000").collect(Collectors.toList());
//            setters_temp = new HashSet<>(s5set);
//            setters.addAll(setters_temp);
//            settersList.add(setters_temp);
//            logger.info(String.format("Setters list of length %d read", setters_temp.size()));

//            N:IO01BI.SETTING
//            N:IO02BI.SETTING
//            N:IO03BI.SETTING
//            N:IO04BI.SETTING
//            N:IO05BI.SETTING
//            N:IO06BI.SETTING
//            N:IO07BI.SETTING
//            N:IO08BI.SETTING
//            N:IO09BI.SETTING
//Z:ACLTST.SETTING

            logger.info(">> DMQ config names: " + ConfigurationLoader.getConfigNames());
            Thread.sleep(500);
            logger.info(">> Authenticating");
            credHandler = new DefaultCredentialHandler();
            // Use reflection
            Class<? extends DefaultCredentialHandler> obj = credHandler.getClass();
            Field field = obj.getDeclaredField("krb5");
            field.setAccessible(true);
            KerberosLoginContext krb5 = (KerberosLoginContext)field.get(credHandler);
            krb5.login();
            logger.info(String.format(">> Authenticated as %s", krb5.getTicket().getClient()));

            daqDefaultClient = new DaqClient();
            //daqDefaultClient.enableSetting(Integer.MAX_VALUE, TimeUnit.SECONDS);
            daqDefaultClient.setCredentialHandler(credHandler);
            DIODMQ.setDaqClient(daqDefaultClient);
            selfTest();
            DIODMQ.enableSettings(true, SettingsState.FOREVER);

            DRFCache.CACHE = new ConcurrentHashMap<>(1024);
            DRFCache.NAMEMAP = new ConcurrentHashMap<>(1024);

            Thread.sleep(500);

            logger.info("Setting up default devices");
            ClassLoader classLoader = Adapter3.class.getClassLoader();

            // Read in IOTA devices
            BufferedReader is = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream("devices.txt")));
            List<String> s1 = is.lines().map(s -> s.split("@")[0] + "@p,5000").collect(Collectors.toList());
            readers = new HashSet<>(s1);
            logger.info(String.format("Device list of length %d read", readers.size()));

            BufferedReader is2 = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream("bpms.txt")));
            List<String> s2 = is2.lines().map(s -> s.split("@")[0] + "@p,5000").collect(Collectors.toList());
            bpms = new HashSet<>(s2);
            logger.info(String.format("BPMs list of length %d read", bpms.size()));

            Stream.of(readers, bpms).flatMap(Collection::stream).forEach(device -> {
                DiscreteRequest req_parsed = (DiscreteRequest) DataRequest.parse(device);
                String device_name = req_parsed.getDevice();
                DRFCache.NAMEMAP.put(device_name, device);
            });

            //logger.info(String.format("Starting cache reading jobs with %d entries", total_size));
            //startReadingDaqJob(s1, "dev_r", 200000);
            //startReadingDaqJob(s2, "bpm_r", 5000);

            //setters = new HashSet<>();
            int cnt = 0;
            List<String> slist = Stream.of("sdevices1.txt", "sdevices2.txt",
                    "sdevices3.txt", "sdevices4.txt", "sdevices5.txt").collect(Collectors.toList());
            //, N:LGINJ.SETTING
            for (String sfile : slist) {
                BufferedReader is3 = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream(sfile)));
                List<String> s3set = is3.lines().map(s -> s.split("@")[0]).collect(Collectors.toList());
                List<String> s3 = is3.lines().map(s -> s.split("@")[0] + "@p,5000").collect(Collectors.toList());
                HashSet<String> setters_temp = new HashSet<>(s3set);
                //setters.addAll(setters_temp);
                settersList.add(setters_temp);
                logger.info(String.format("Setters list (%s) of length (%d) read", sfile, setters_temp.size()));

                Stream.of(setters_temp).flatMap(Collection::stream).forEach(device -> {
                    DiscreteRequest req_parsed = (DiscreteRequest) DataRequest.parse(device);
                    String device_name = req_parsed.getDevice();
                    DRFCache.NAMEMAP.put(device_name, device);
                });

                //Thread.sleep(500);
                //startReadingDaqJob(s3, String.format("Reader %d", cnt), 200000);
                Thread.sleep(500);
                startSettingDaqJob(s3set);
                cnt += 1;
            }

//            List<String> slist2 = Stream.of( "sdevices6.txt").collect(Collectors.toList());
//            for (String sfile : slist2) {
//                BufferedReader is3 = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream(sfile)));
//                List<String> list_status = is3.lines().map(s -> s).collect(Collectors.toList());
//                List<String> list_control = is3.lines().map(s -> s.split(".")[0] + "CONTROL").collect(Collectors.toList());
//                HashSet<String> setters_temp = new HashSet<>(s3set);
//                //setters.addAll(setters_temp);
//                settersList.add(setters_temp);
//                logger.info(String.format("Setters list (%s) of length (%d) read", sfile, setters_temp.size()));
//
//                Stream.of(setters_temp).flatMap(Collection::stream).forEach(device -> {
//                    DiscreteRequest req_parsed = (DiscreteRequest) DataRequest.parse(device);
//                    String device_name = req_parsed.getDevice();
//                    DRFCache.NAMEMAP.put(device_name, device);
//                });
//
//                Thread.sleep(500);
//                startSettingDaqJob(list_status);
//
//                cnt += 1;
//            }

            logger.info("Setting up HTTP relay");
            setupUndertowRelay();

            logger.info("================================================");
            logger.info(">>>READY<<<");
        } catch(Exception e){
            System.out.println("Startup exception");
            System.out.println(e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void selfTest() {
        try {
            logger.info(" ==== Running a self-test ==== ");
            boolean outcome = DIODMQ.enableSettings(true, SettingsState.FOREVER);
            logger.info(String.format("Settings enabled?: %s", outcome));
            Thread.sleep(500);
            logger.info("== Read test ==");
            TimedNumber temp = DIODMQ.readDevice("M:OUTTMP@I");
            Thread.sleep(500);
            logger.info("M:OUTTMP@I = " + temp + " | time: " + DIODMQ.computedTime);
            TimedNumber hum = DIODMQ.readDevice("M:OUTHUM.READING@I");
            logger.info("M:OUTHUM.READING@I = " + hum + " | time: " + DIODMQ.computedTime);
            logger.info(String.format("It is %f %s outside and humidity is %f %s",
                    temp.doubleValue(), temp.getUnit(), hum.doubleValue(), hum.getUnit()));
            logger.info("Read test - OK");

            Thread.sleep(500);

            logger.info("Write test");
            TimedError status = DIODMQ.setDevice("Z_ACLTST", 5.5);
            logger.info("Z_ACLTST setting status = " + status + " | time: " + DIODMQ.computedTime);
            Thread.sleep(500);
            TimedNumber data = DIODMQ.readDevice("Z:ACLTST@I");
            logger.info(String.format("Z:ACLTST readback 1: %s",data));
            status = DIODMQ.setDevice("Z:ACLTST.SETTING", 5.6);
            logger.info("Z:ACLTST.SETTING setting status = " + status + " | time: " + DIODMQ.computedTime);
            TimedNumber data2 = DIODMQ.readDevice("Z:ACLTST.READING@I");
            logger.info(String.format("Z:ACLTST readback 2: %s",data2));
            if (data.doubleValue() == 5.5 && (data2.doubleValue() - 5.6) < 1e-6) {
                logger.info("Write test - OK");
            } else {
                logger.info("Write test - FAILED");
            }
            //DIODMQ.enableSettings(false, SettingsState.FOREVER);
        } catch(Exception e){
            System.out.println("Error running self-test - exiting...");
            System.out.println(e);
            e.printStackTrace();
            System.exit(1);
        }
    }


    private static void startReadingDaqJob(List<String> devices, String name, int freq) {
        class ServiceObserver implements ConnectionListener {
            private ServiceObserver() {
            }

            public void connectionChanged(boolean var1) {
                if (var1) {
                    logger.info("BROKER CONNECTED");
                } else {
                    logger.info("BROKER DISCONNECTED");
                }

            }
        }

        if (daqReadingClient == null) {
            daqReadingClient = new DaqClient(preset);
            daqReadingClient.setCredentialHandler(credHandler);
            daqReadingClient.addConnectionListener(new ServiceObserver());
        }

        HashSet<String> hs = new HashSet<>(devices);
        ReadingJob readingJob = daqReadingClient.createReadingJob(hs);
        DataUpdateCallback cb = new DataUpdateCallback(devices, name, freq);
        if (callbacks.putIfAbsent(name, cb) != null) {
            throw new IllegalArgumentException("Job name not unique!");
        }
        readingJob.addDataCallback(cb);
        logger.info("Created new DAQClient reading job");
        logger.info("Authorized as:" + daqReadingClient);
    }

    private static void startSettingDaqJob(List<String> devices) {
        //if (daqSettingClient == null) {
        daqSettingClient = new DaqClient(preset);
        daqSettingClient.setCredentialHandler(credHandler);
        daqSettingClient.enableSetting(SettingsState.FOREVER, TimeUnit.MINUTES);
        //}
        daqSettingClients.add(daqSettingClient);
        SettingJob sj = daqSettingClient.createSettingJob(new HashSet<>(devices));
        SettingCallback cb = new SettingCallback();
        sj.addDataCallback(cb);
        settingJobs.add(sj);
        settingCallbackMap.put(sj, cb);
        //settingCallback.getData();
        //DIODMQSettingJob test = new DIODMQSettingJob(daqSettingClient, "test");
        //DaqClient dc = new DaqClient();
        //dc.setCredentialHandler(new DefaultCredentialHandler());
        //DIODMQ.setDaqClient(dc);
        //DIODMQ.enableSettings(true, SettingsState.FOREVER);
        logger.info(String.format("Client created for %d devices, settings: %s", devices.size(),daqSettingClient.isSettingEnabled()));
    }

    private static void setupUndertowRelay() {
        Undertow.Builder server = Undertow.builder();
        // avoid concurrency issues
        server.setIoThreads(1);
        server.setWorkerThreads(1);
        server.setDirectBuffers(true);
        server.setBufferSize(1024 * 64 - 20);
        server.addHttpListener(8080, "localhost");

        server.setSocketOption(Options.TCP_NODELAY, true);
        server.setSocketOption(Options.REUSE_ADDRESSES, true);
        server.setSocketOption(Options.KEEP_ALIVE, true);
        server.setSocketOption(Options.SSL_ENABLED, false);
        server.setServerOption(UndertowOptions.ENABLE_HTTP2 , true);
        //server.setHandler(new AllowedMethodsHandler(new UndertowPOSTHandler(), new HttpString("POST")));
//        HttpHandler pathHandler = Handlers.path(Handlers.redirect("/"))
//                .addPrefixPath("/", exchange -> exchange.getResponseSender().send("echo"));
//        server.se

        HttpHandler mainHandler = new UndertowHandler();
        HttpHandler errorHandler = new ErrorHandler(mainHandler);
        server.setHandler(errorHandler);
//        HttpHandler encodingHandler = new EncodingHandler.Builder().build(null)
//                .wrap(mainHandler);
//        server.setHandler(encodingHandler);

        server.build().start();
    }
}


