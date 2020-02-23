import gov.fnal.controls.daq.acquire.SettingsState;
import gov.fnal.controls.service.dmq.*;
import gov.fnal.controls.tools.dio.DIODMQ;
import gov.fnal.controls.tools.logging.LogFormatter;
import gov.fnal.controls.tools.logging.LogInit;
import gov.fnal.controls.tools.timed.*;
import io.undertow.Undertow;
import io.undertow.server.handlers.AllowedMethodsHandler;
import io.undertow.util.HttpString;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class DRFCache {
    public static ConcurrentHashMap<String, TimedNumber> CACHE = null;
    public static ConcurrentHashMap<String, String> NAMEMAP = null;
}


public class Adapter2 {

    private static final Logger logger = Logger.getLogger(Adapter2.class.getName());
    public static DaqClient daqReadingClient = null;
    public static DaqClient daqSettingClient = null;
    public static ConcurrentHashMap<String, DataUpdateCallback> callbacks = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        System.setProperty("dmq.heartbeat-rate", "500");
        System.setProperty("dmq.amqp-heartbeat-rate", "500");
        System.setProperty("dmq.max-idle-time", "10500");

        Handler handlerObj = new ConsoleHandler();
        handlerObj.setFormatter(new LogFormatter());
        handlerObj.setLevel(Level.ALL);
        logger.addHandler(handlerObj);
        logger.setUseParentHandlers(false);
        logger.log(Level.INFO, "ACNET2Py relay starting up");
        LogInit.initializeLogs();

        logger.info("TEMP: " + System.getProperty("java.io.tmpdir"));
        logger.info("CWD: " + System.getProperty("user.dir"));

        selfTest();

        try {
            logger.info("Setting up default devices");
            Charset charset = Charset.defaultCharset();
            ClassLoader classLoader = Adapter2.class.getClassLoader();

            // Read in IOTA devices
            URL resource = classLoader.getResource("devices.txt");
            if (resource == null) {
                throw new IllegalArgumentException("Devices not found!");
            }
            Path filePath = new File(resource.getFile()).toPath();
            List<String> s1 = Files.readAllLines(filePath, charset).stream()
                    .map(s -> s.split("@")[0] + "@p,200").collect(Collectors.toList());
            HashSet<String> devices = new HashSet<>(s1);

            URL resource2 = classLoader.getResource("bpms.txt");
            if (resource2 == null) {
                throw new IllegalArgumentException("Devices not found!");
            }
            Path filePath2 = new File(resource2.getFile()).toPath();
            List<String> s2 = Files.readAllLines(filePath2, charset).stream()
                    //.map(s -> {if (s.contains("@")) {return s;} else {return s + "@p,1000";}})
                    .map(s -> s.split("@")[0]+ "@p,5000")
                    .filter(s -> s.charAt(7) == 'V').collect(Collectors.toList());
            HashSet<String> bpms = new HashSet<>(s2);

            int total_size = devices.size()+bpms.size();
            DRFCache.CACHE = new ConcurrentHashMap<>(total_size);
            DRFCache.NAMEMAP = new ConcurrentHashMap<>(total_size);
            Stream.of(devices, bpms).flatMap(Collection::stream)
                    .forEach(device -> {
                        String d = device.split("@")[0];
                        String d2 = d.split("\\[")[0];
                        DRFCache.NAMEMAP.put(d2, device);
                    });

            logger.info(String.format("Starting cache jobs with %d entries", total_size));
            startReadingDaqJob(s1, "devices", 50000);
            startReadingDaqJob(s2, "bpms", 100);
//            for (String dev : s2) {
//                startReadingDaqJob(new ArrayList<String>(Arrays.asList(dev)), "bpms" + dev, 100);
//                Thread.sleep(1000);
//            }

            startSettingDaqJob();

            logger.info("Setting up HTTP relay");
            setupUndertowRelay();

            logger.info(">>>READY<<<");
        } catch(Exception e){
            System.out.println("Startup exception");
            System.out.println(e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void selfTest() {
        DIODMQ.enableSettings(true, SettingsState.FOREVER);
        logger.info(String.format("SETTINGS STATUS: %s",DIODMQ.isSettingEnabled()));
        try {
            logger.info(" ==== Running a self-test ==== ");
            logger.info("Read test");
            TimedNumber temp = DIODMQ.readDevice("M:OUTTMP@I");
            logger.info(String.format("It is %f %s outside...yikes", temp.doubleValue(), temp.getUnit()));
            logger.info("Read test - OK");

            logger.info("Write test");
            DIODMQ.setDevice("Z_ACLTST", 5.5);
            TimedNumber data = DIODMQ.readDevice("Z:ACLTST@I");
            logger.info(String.format("Z:ACLTST readback 1: %s",data));
            DIODMQ.setDevice("Z_ACLTST", 5.6);
            TimedNumber data2 = DIODMQ.readDevice("Z:ACLTST@I");
            logger.info(String.format("Z:ACLTST readback 2: %s",data2));
            if (data.doubleValue() == 5.5 && (data2.doubleValue() - 5.6) < 1e-6) {
                logger.info("Write test - OK");
            } else {
                logger.info("Write test - FAILED");
            }
            DIODMQ.enableSettings(false, SettingsState.FOREVER);
        } catch(Exception e){
            System.out.println("Error running self-test - exiting...");
            System.out.println(e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void startReadingDaqJob(List<String> devices, String name, int freq) {
        if (daqReadingClient == null) {
            daqReadingClient = new DaqClient();
            daqReadingClient.setCredentialHandler(new DefaultCredentialHandler());
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

    private static void startSettingDaqJob() {
        daqSettingClient = new DaqClient();
        daqSettingClient.setCredentialHandler(new DefaultCredentialHandler());
        DIODMQ.setDaqClient(daqSettingClient);
        daqSettingClient.enableSetting(SettingsState.FOREVER, TimeUnit.MINUTES);
        DIODMQ.enableSettings(true, SettingsState.FOREVER);
        logger.info("Client created, setting enabled: " + daqSettingClient.isSettingEnabled());
    }

    private static void setupUndertowRelay() {
        Undertow.Builder server = Undertow.builder();
        server.addHttpListener(8080, "localhost");
        server.setHandler(new AllowedMethodsHandler(new UndertowPOSTHandler(), new HttpString("POST")));
        server.build().start();
    }


}

