import com.fasterxml.jackson.databind.ObjectMapper;
import gov.fnal.controls.daq.acquire.SettingsState;
import gov.fnal.controls.service.dmq.*;
import gov.fnal.controls.tools.dio.DIODMQ;
import gov.fnal.controls.tools.logging.LogFormatter;
import gov.fnal.controls.tools.logging.LogInit;
import gov.fnal.controls.tools.timed.*;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.AllowedMethodsHandler;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

class DRFCache {
    public static ConcurrentHashMap<String, TimedNumber> CACHE = null;
    public static ConcurrentHashMap<String, String> NAMEMAP = null;
}


public class Adapter2 {

    private static final Logger logger = Logger.getLogger(Adapter2.class.getName());
    public static DaqClient daqClient = null;

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
        } catch(Exception e){
            System.out.println("Error running self-test - exiting...");
            System.out.println(e);
            e.printStackTrace();
            System.exit(1);
        }

        try {
            logger.info("Setting up default devices");
            Charset charset = Charset.defaultCharset();

            // Read in IOTA devices
            Path filePath = new File("devices.txt").toPath();
            List<String> stringList = new ArrayList<>();
            for (String s:Files.readAllLines(filePath, charset)) {
                if (s.contains("@")) {
                    stringList.add(s);
                } else {
                    stringList.add(s+"@p,200");
                }
            }
            HashSet<String> devices = new HashSet<>(stringList);

            // Read in IOTA devices
            Path filePath2 = new File("bpms.txt").toPath();
            List<String> s2 = Files.readAllLines(filePath2, charset).stream()
                    //.map(s -> {if (s.contains("@")) {return s;} else {return s + "@p,1000";}})
                    .map(s -> s.split("@")[0]+ "@p,5000")
                    .filter(s -> s.charAt(7) == 'V')
                    .collect(Collectors.toList());
            HashSet<String> bpms = new HashSet<>(s2);

            int total_size = devices.size()+bpms.size();
            DRFCache.CACHE = new ConcurrentHashMap<>(total_size);
            DRFCache.NAMEMAP = new ConcurrentHashMap<>(total_size);
            devices.forEach(device -> {String d = device.split("@")[0];
                String d2 = d.split("\\[")[0];
                DRFCache.NAMEMAP.put(d2, device);
            });
            bpms.forEach(device -> {String d = device.split("@")[0];
                String d2 = d.split("\\[")[0];
                DRFCache.NAMEMAP.put(d2, device);
            });

            logger.info(String.format("Starting cache jobs with %d entries", total_size));
            //startDaqJob(stringList, "devices", 50000);
            //startDaqJob(s2, "bpms", 1000);
            for (String dev : s2) {
                startDaqJob(new ArrayList<String>(Arrays.asList(dev)), "bpms" + dev, 100);
                Thread.sleep(1000);
            }

            logger.info("Setting up HTTP relay");
            setupUndertowRelay();

            logger.info(">>>READY<<<");
        } catch(Exception e){
            System.out.println("OOF");
            System.out.println(e);
            e.printStackTrace();
        }
    }

    private static void startDaqJob(List<String> devices, String name, int freq) {
        if (daqClient == null) {
            logger.info("Creating new DAQClient");
            daqClient = new DaqClient();
            daqClient.setCredentialHandler(new DefaultCredentialHandler());
            daqClient.enableSetting(SettingsState.FOREVER, TimeUnit.MINUTES);
            logger.info("Client created, setting enabled: " + daqClient.isSettingEnabled());
        }
        HashSet<String> hs = new HashSet<>(devices);
        ReadingJob readingJob = daqClient.createReadingJob(hs);
        readingJob.addDataCallback(new DataUpdateDMQCallback(devices, name, freq));
        logger.info("Authorized as:" + daqClient.getCredentials().toString());
    }

    private static void setupUndertowRelay() {
        Undertow.Builder server = Undertow.builder();
        server.addHttpListener(8080, "localhost");
        server.setHandler(new AllowedMethodsHandler(new UndertowPOSTHandler(), new HttpString("POST")));
        server.build().start();
    }

    public static String convertTimedNumber(TimedNumber v) {
        if (v instanceof TimedDouble) {
            return Double.toString(v.doubleValue());
        } else if (v instanceof TimedDoubleArray) {
            double[] doubleArray = ((TimedDoubleArray) v).doubleArray();
            ByteBuffer buf = ByteBuffer.allocate(Double.SIZE / Byte.SIZE * doubleArray.length);
            buf.asDoubleBuffer().put(doubleArray);
            return Base64.getEncoder().encodeToString(buf.array());
        } else if (v instanceof TimedBasicStatus) {
            return v.toString();
        } else if (v instanceof TimedError) {
            return v.toString();
        } else {
            System.out.println("Can't stringify:" + v.getClass());
            System.out.println(v);
            throw new IllegalArgumentException();
        }
    }
}

class DataUpdateDMQCallback extends TimedNumberCallback {

    ConcurrentHashMap<String, TimedNumber> results = DRFCache.CACHE;
    ConcurrentHashMap<String, Integer> errors;
    HashSet<String> devices;
    int num_updates = 0;
    int num_errors = 0;
    int freq;
    String name;

    public DataUpdateDMQCallback(List<String> devices, String name, int freq) {
        this.devices = new HashSet<>(devices);
        this.errors = new ConcurrentHashMap<>(devices.size());
        devices.forEach(s -> this.errors.put(s,0));
        this.freq = freq;
        this.name = name;
    }

    public void dataChanged(String var1, TimedNumber var2) {
        //System.out.println("Callback:" + var1 + "|" + var2);
        num_updates++;
        if (num_updates % this.freq == 0) {
            System.out.println(String.format("%s | updates: %07d | errors: %05d | %s",
                    this.name, num_updates, num_errors, var2));
        }
        if (var2 instanceof TimedError) {
            num_errors++;
            if (var1 != null) {
                errors.put(var1, errors.get(var1) + 1);
            } else {
                System.out.println("NULL Callback:" + var2);
            }
            int error = ((TimedError) var2).getErrorNumber();
            if ((error == -89) || (error == -107) || (error == 1) || (error == 3)) {
                return; // These are common errors
            }
            System.out.println(((TimedError) var2).getErrorNumber() + " | " + var2 + " | " + var1);
        }
        results.put(var1, var2);
    }
}

class UndertowPOSTHandler implements io.undertow.server.HttpHandler {

    ObjectMapper objectMapper;

    public UndertowPOSTHandler() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        long t0 = System.nanoTime();
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        long t1 = System.nanoTime();
        System.out.println(">In blocking thread:" + (t1-t0)/1e3);

        exchange.startBlocking();
        InputStream is = exchange.getInputStream();
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = is.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        String message = result.toString(StandardCharsets.UTF_8.name());

        long t2 = System.nanoTime();
        System.out.println(">Request read:" + message + ' ' + (t2-t1)/1e3);

        Message r;
        try {
            r = this.objectMapper.readValue(message, Message.class);
        } catch (Exception ex) {
            ex.printStackTrace();
            abort("MALFORMED JSON OBJECT", exchange);
            return;
        }
        String requestType = r.requestType;
        String requestDRF = r.request;
        //System.out.println(">>T: " + requestType + " | M: " + requestDRF);
        if (requestDRF == null || requestType == null) {
            abort("MALFORMED JSON OBJECT", exchange);
            return;
        }
        //checkRequest(requestDRF);
        long t3 = System.nanoTime();
        System.out.println(String.format("> %f - Request parsed: %s %s ",
                (t3-t2)/1e3,requestType,requestDRF));
        if (!checkRequest(requestDRF)) {abort("INVALID REQUEST - REJECTED BY SANITY CHECKS", exchange);};

        TimedNumber data;
        HashMap<String, TimedNumber> obj_map;
        long t4 = 0;
        try {
            if (requestType.equalsIgnoreCase("V1_DRF2_READ_MULTI_CACHED") ||
                    requestType.equalsIgnoreCase("V1_DRF2_READ_SINGLE_CACHED") ||
                    requestType.equalsIgnoreCase("V1_DRF2_READ_CACHED")) {
                String[] requests = requestDRF.split(";");
                obj_map = new HashMap<>(requests.length);
                for (String req : requests) {
                    // Have to use concurrent methods...grrr
                    String a = DRFCache.NAMEMAP.getOrDefault(req.split("@")[0],null);
                    if (a == null) {
                        data = DRFCache.CACHE.getOrDefault(req,null);
                        r.response = req;
                    } else {
                        data = DRFCache.CACHE.getOrDefault(a, null);
                        r.response = a;
                    }
                    if (data == null) {
                        System.out.println(DRFCache.CACHE.keySet());
                        abort(String.format("INVALID REQUEST - DEVICE %s NOT IN CACHE",req), exchange);
                        return;
                    } else {
                        obj_map.put(req, data);
                    }
                }
                //System.out.println(String.format("> Complete! (WITH CACHE)"));
            } else if (requestType.equalsIgnoreCase("V1_DRF2_READ_SINGLE")) {
                obj_map = new HashMap<>(1);
                long startTime = System.nanoTime();
                data = DIODMQ.readDevice(requestDRF);
                long endTime = System.nanoTime();
                long duration = (endTime - startTime);
                System.out.println(String.format(">(%s) complete! (%.4f ms)", requestType, duration / 1e6));

                if (data == null) {
                    abort("INVALID REQUEST - REJECTED BY ACNET", exchange);
                    return;
                }
                obj_map.put(requestDRF, data);
            } else if (requestType.equalsIgnoreCase("V1_DRF2_SET_SINGLE")) {
                obj_map = new HashMap<>(1);
                long startTime = System.nanoTime();
                try {
                    double val = Double.parseDouble(r.requestValue);
                    data = DIODMQ.setDevice(requestDRF,  val);
                } catch (NumberFormatException e) {
                    data = DIODMQ.setDevice(requestDRF,  r.requestValue);
                }

                //System.out.println(data);
                //data = DIODMQ.readDevice(requestDRF);
                long endTime = System.nanoTime();
                long duration = (endTime - startTime);
                System.out.println(String.format(">(%s) complete! (%.4f ms)", requestType, duration / 1e6));

                if (data == null) {
                    abort("INVALID REQUEST - REJECTED BY ACNET", exchange);
                    return;
                }
                obj_map.put(requestDRF, data);

            } else {
                abort("INVALID REQUEST TYPE", exchange);
                return;
            }
            r.responseTime = Instant.now().toString();

            t4 = System.nanoTime();
            System.out.println(">Request processed:" + (t4-t3)/1e3);

            HashMap<String, String> map = new HashMap<>(obj_map.size());
            obj_map.forEach((k, v) -> map.put(k, Adapter2.convertTimedNumber(v)));
            r.responseJson = map;
        } catch (Exception e) {
            e.printStackTrace();
            abort("INTERNAL FAILURE: " + e.getMessage(), exchange);
            System.exit(1);
        }

        long t5 = System.nanoTime();
        System.out.println(">Request serialized:" + (t5-t4)/1e3);

        try {
            //System.out.println("Writing json");
            //String response = objectMapper.writeValueAsString(r);
            //int len = response.length;
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
            exchange.setStatusCode(200);
            objectMapper.writeValue(exchange.getOutputStream(),r);
        } catch (Exception e) {
            e.printStackTrace();
            abort("INTERNAL SERIALIZATION FAILURE: " + e.getMessage(), exchange);
            System.exit(1);
        } finally {
            System.out.println(String.format("Total time (ms): %f | Length: ?", (System.nanoTime() - t0) / 1e6));
            exchange.endExchange();
        }
    }

    boolean checkRequest(String requestDRF) {
        return ((requestDRF.contains(":") || requestDRF.contains("|") || requestDRF.contains("_")) &&
                Charset.forName(StandardCharsets.US_ASCII.name()).newEncoder().canEncode(requestDRF));
    }

    void abort(String response, final HttpServerExchange exchange) throws IOException {
        System.err.println("Aborting! " + response);
        exchange.startBlocking();
        exchange.setStatusCode(403);
        exchange.getResponseSender().send(response);
        exchange.endExchange();
        return;
    }
}