//import gov.fnal.controls.daq.acquire.SettingsState;
//import gov.fnal.controls.service.dmq.*;
//import gov.fnal.controls.tools.dio.DIODMQ;
//import gov.fnal.controls.tools.logging.LogFormatter;
//import gov.fnal.controls.tools.logging.LogInit;
//import gov.fnal.controls.tools.timed.*;
//import io.undertow.Undertow;
//
//import java.io.*;
//import java.net.URI;
//import java.net.URL;
//import java.nio.charset.Charset;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.TimeUnit;
//import java.util.logging.*;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
////class DRFCache {
////    public static ConcurrentHashMap<String, TimedNumber> CACHE = null;
////    public static ConcurrentHashMap<String, String> NAMEMAP = null;
////}
//
//
//public class Adapter2 {
//
//    private static final Logger logger = Logger.getLogger(Adapter2.class.getName());
//    public static DaqClient daqReadingClient = null;
//    public static DaqClient daqSettingClient = null;
//    public static SettingJob settingJob = null;
//    public static TimedNumberCallback settingCallback = null;
//    public static ConcurrentHashMap<String, DataUpdateCallback> callbacks = new ConcurrentHashMap<>();
//    public static HashSet<String> bpms = null;
//    public static HashSet<String> setters = null;
//
//    public static void main(String[] args) {
//        //System.setProperty("dmq.heartbeat-rate", "1000");
//        //System.setProperty("dmq.amqp-heartbeat-rate", "1000");
//        //System.setProperty("dmq.max-idle-time", "15000");
//
//        LogInit.initializeLogs(); // debugs
////        Logger master = Logger.getLogger("");
////        for (Handler h : master.getHandlers()) {
////            if (h instanceof FileHandler) {
////                master.removeHandler(h);
////            }
////        }
//        //LogManager.getLogManager().reset();
//        //StreamHandler handlerObj = new StreamHandler(System.out, new LogFormatter());
//        //handlerObj.setLevel(Level.ALL);
//        //logger.addHandler(handlerObj);
//        logger.setUseParentHandlers(true);
//        logger.log(Level.INFO, "ACNET2Py relay starting up");
//        logger.info("TEMP: " + System.getProperty("java.io.tmpdir"));
//        logger.info("CWD: " + System.getProperty("user.dir"));
//
//        try {
//            logger.info("Setting up default devices");
//            Charset charset = Charset.defaultCharset();
//            ClassLoader classLoader = Adapter2.class.getClassLoader();
//
//            // Read in IOTA devices
////            URI uri = ClassLoader.getSystemResource("holder").toURI();
////            logger.info(String.format("devices path: %s", uri.toString()));
////            String mainPath = Paths.get(uri).toString();
////            Path filePath = Paths.get(mainPath ,"devices.txt");
//
////            URL resource = classLoader.getResource("devices.txt");
////            if (resource == null) {
////                throw new IllegalArgumentException("Devices not found!");
////            } else {
////                logger.info(String.format("devices path: %s", resource.getFile().toString()));
////            }
////            Path filePath = new File(resource.getFile()).toPath();
//            //Path filePath = Paths.get(resource.toURI());
//
//            //Path filePath = new File(Adapter2.class.getResource("/devices.txt").getFile()).toPath();
//
////            List<String> s1 = Files.readAllLines(filePath, charset).stream()
////                    .map(s -> s.split("@")[0] + "@p,5000").collect(Collectors.toList());
////            HashSet<String> devices = new HashSet<>(s1);
//            BufferedReader is = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream("devices.txt")));
//            List<String> s1 = is.lines().map(s -> s.split("@")[0] + "@p,5000").collect(Collectors.toList());
//            HashSet<String> devices = new HashSet<>(s1);
//            logger.info("Device list read");
////            List<String> s1 = Files.readAllLines(filePath, charset).stream()
////                    .map(s -> s.split("@")[0] + "@p,5000").collect(Collectors.toList());
////            HashSet<String> devices = new HashSet<>(s1);
//
////            URL resource2 = classLoader.getResource("bpms.txt");
////            if (resource2 == null) {
////                throw new IllegalArgumentException("Devices not found!");
////            } else {
////                logger.info(String.format("bpms path: %s", resource2.toString()));
////            }
////            Path filePath2 = new File(resource2.getFile()).toPath();
//
////            URI uri2 = ClassLoader.getSystemResource("holder").toURI();
////            String mainPath2 = Paths.get(uri2).toString();
////            Path filePath2 = Paths.get(mainPath2 ,"bpms.txt");
//
//            //Path filePath2 = new File(Adapter2.class.getResource("/bpms.txt").getFile()).toPath();
////            List<String> s2 = Files.readAllLines(filePath2, charset).stream()
////                    //.map(s -> {if (s.contains("@")) {return s;} else {return s + "@p,1000";}})
////                    .map(s -> s.split("@")[0]+ "@p,5000")
////                    .filter(s -> s.charAt(7) == 'V').collect(Collectors.toList());
////            bpms = new HashSet<>(s2);
//
//            BufferedReader is2 = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream("bpms.txt")));
//            List<String> s2 = is2.lines().map(s -> s.split("@")[0] + "@p,5000").collect(Collectors.toList());
//            bpms = new HashSet<>(s2);
//            logger.info("BPMs list read");
//
////            URL resource3 = classLoader.getResource("setting_devices.txt");
////            if (resource3 == null) {
////                throw new IllegalArgumentException("Devices not found!");
////            }
////            Path filePath3 = new File(resource3.getFile()).toPath();
////            List<String> s3 = new ArrayList<>(Files.readAllLines(filePath3, charset));
////            setters = new HashSet<>(s3);
//
//            BufferedReader is3 = new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream("setting_devices.txt")));
//            List<String> s3 = is3.lines().map(s -> s.split("@")[0] + "@p,5000").collect(Collectors.toList());
//            setters = new HashSet<>(s3);
//            logger.info("Setters list read");
//
//            selfTest();
//
//            int total_size = devices.size()+bpms.size();
//            DRFCache.CACHE = new ConcurrentHashMap<>(total_size);
//            DRFCache.NAMEMAP = new ConcurrentHashMap<>(total_size);
//            Stream.of(devices, bpms).flatMap(Collection::stream)
//                    .forEach(device -> {
//                        String d = device.split("@")[0];
//                        String d2 = d.split("\\[")[0];
//                        DRFCache.NAMEMAP.put(d2, device);
//                    });
//
//            logger.info(String.format("Starting cache jobs with %d entries", total_size));
//            //startReadingDaqJob(s1, "devices", 50000);
//            //startReadingDaqJob(s2, "bpms", 100);
////            for (String dev : s2) {
////                startReadingDaqJob(new ArrayList<String>(Arrays.asList(dev)), "bpms" + dev, 100);
////                Thread.sleep(77);
////            }
//            startSettingDaqJob(s3);
//
//            logger.info("Setting up HTTP relay");
//            setupUndertowRelay();
//
//            logger.info(">>>READY<<<");
//        } catch(Exception e){
//            System.out.println("Startup exception");
//            System.out.println(e);
//            e.printStackTrace();
//            System.exit(1);
//        }
//    }
//
//    public static void selfTest() {
//        DIODMQ.enableSettings(true, SettingsState.FOREVER);
//        logger.info(String.format("SETTINGS STATUS: %s",DIODMQ.isSettingEnabled()));
//        try {
//            logger.info(" ==== Running a self-test ==== ");
//            logger.info("Read test");
//            TimedNumber temp = DIODMQ.readDevice("M:OUTTMP@I");
//            logger.info(String.format("It is %f %s outside...yikes", temp.doubleValue(), temp.getUnit()));
//            logger.info("Read test - OK");
//            Thread.sleep(1000);
//            logger.info("Write test");
//            DIODMQ.setDevice("Z_ACLTST", 5.5);
//            Thread.sleep(1000);
//            TimedNumber data = DIODMQ.readDevice("Z:ACLTST@I");
//            logger.info(String.format("Z:ACLTST readback 1: %s",data));
//            DIODMQ.setDevice("Z_ACLTST", 5.6);
//            TimedNumber data2 = DIODMQ.readDevice("Z:ACLTST@I");
//            logger.info(String.format("Z:ACLTST readback 2: %s",data2));
//            if (data.doubleValue() == 5.5 && (data2.doubleValue() - 5.6) < 1e-6) {
//                logger.info("Write test - OK");
//            } else {
//                logger.info("Write test - FAILED");
//            }
//            DIODMQ.enableSettings(false, SettingsState.FOREVER);
//        } catch(Exception e){
//            System.out.println("Error running self-test - exiting...");
//            System.out.println(e);
//            e.printStackTrace();
//            System.exit(1);
//        }
//    }
//
//    private static void startReadingDaqJob(List<String> devices, String name, int freq) {
//        if (daqReadingClient == null) {
//            daqReadingClient = new DaqClient();
//            daqReadingClient.setCredentialHandler(new DefaultCredentialHandler());
//        }
//        HashSet<String> hs = new HashSet<>(devices);
//        ReadingJob readingJob = daqReadingClient.createReadingJob(hs);
//        DataUpdateCallback cb = new DataUpdateCallback(devices, name, freq);
//        if (callbacks.putIfAbsent(name, cb) != null) {
//            throw new IllegalArgumentException("Job name not unique!");
//        }
//        readingJob.addDataCallback(cb);
//        logger.info("Created new DAQClient reading job");
//        logger.info("Authorized as:" + daqReadingClient);
//    }
//
//    private static void startSettingDaqJob(List<String> devices) {
//        daqSettingClient = new DaqClient();
//        daqSettingClient.setCredentialHandler(new DefaultCredentialHandler());
//        daqSettingClient.enableSetting(SettingsState.FOREVER, TimeUnit.MINUTES);
//        settingJob = daqSettingClient.createSettingJob(new HashSet<>(devices));
//        settingCallback = new SettingCallback();
//        settingJob.addDataCallback(settingCallback);
//        //DIODMQSettingJob test = new DIODMQSettingJob(daqSettingClient, "test");
//        DaqClient dc = new DaqClient();
//        dc.setCredentialHandler(new DefaultCredentialHandler());
//        DIODMQ.setDaqClient(dc);
//        DIODMQ.enableSettings(true, SettingsState.FOREVER);
//        logger.info("Client created, setting enabled: " + daqSettingClient.isSettingEnabled());
//    }
//
//    private static void setupUndertowRelay() {
//        Undertow.Builder server = Undertow.builder();
//        server.setIoThreads(8);
//        server.setWorkerThreads(8*8);
//        server.setDirectBuffers(true);
//        server.setBufferSize(16364);
//        server.addHttpListener(8080, "localhost");
//        //server.setHandler(new AllowedMethodsHandler(new UndertowPOSTHandler(), new HttpString("POST")));
//        server.setHandler(new UndertowHandler());
//        server.build().start();
//    }
//}
//
