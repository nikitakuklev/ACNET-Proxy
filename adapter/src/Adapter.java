//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.google.gson.*;
//import com.sun.net.httpserver.HttpExchange;
//import com.sun.net.httpserver.HttpHandler;
//import com.sun.net.httpserver.HttpServer;
//import gov.fnal.controls.service.dmq.*;
//import gov.fnal.controls.service.dmq.DefaultCredentialHandler;
//import gov.fnal.controls.tools.dio.*;
//import gov.fnal.controls.tools.timed.*;
//import gov.fnal.controls.tools.logging.*;
//
//import com.dslplatform.json.*;
//import com.dslplatform.json.runtime.Settings;
//import io.undertow.Undertow;
//import io.undertow.server.HttpServerExchange;
//import io.undertow.server.handlers.AllowedMethodsHandler;
//import io.undertow.util.Headers;
//import io.undertow.util.HttpString;
//import org.ietf.jgss.GSSException;
//
//import javax.security.auth.Subject;
//import javax.security.auth.kerberos.KerberosPrincipal;
//import java.io.*;
//import java.lang.reflect.Type;
//import java.math.RoundingMode;
//import java.net.InetSocketAddress;
//import java.nio.ByteBuffer;
//import java.nio.charset.Charset;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.text.DecimalFormat;
//import java.time.Instant;
//import java.util.*;
//import java.util.List;
//import java.util.concurrent.*;
//import java.util.logging.ConsoleHandler;
//import java.util.logging.Handler;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//
//public class Adapter {
//
//    private static final Logger logger = Logger.getLogger(Adapter.class.getName());
//    public static ConcurrentHashMap<String, TimedNumber> CACHE = null;
//    public static HashSet<String> devices = null;
//    public static HashSet<String> bpms = null;
//    public static DaqClient daqClient = null;
//
//    public static void main(String[] args) {
//        System.setProperty("dmq.heartbeat-rate", "500");
//        System.setProperty("dmq.amqp-heartbeat-rate", "500");
//        System.setProperty("dmq.max-idle-time", "10500");
//
//        Handler handlerObj = new ConsoleHandler();
//        handlerObj.setFormatter(new LogFormatter());
//        handlerObj.setLevel(Level.ALL);
//        logger.addHandler(handlerObj);
//        logger.setUseParentHandlers(false);
//        logger.log(Level.INFO, "ACNET2Py relay starting up");
//        LogInit.initializeLogs();
//
//        try {
//            logger.info("TEMP: " + System.getProperty("java.io.tmpdir"));
//            logger.info("CWD: " + System.getProperty("user.dir"));
//            logger.info("Running a self-test");
//            //TimedNumber temp = DIODMQ.readDevice("M:OUTTMP@I");
//            //System.out.println(String.format("It is %f %s outside...yikes", temp.doubleValue(), temp.getUnit()));
//
//            //System.out.println("Going SETTINGS HOT");
//            //DIODMQ.enableSettings(true, SettingsState.FOREVER);
//
//            //DIODMQ.setDevice("Z_ACLTST", 5.5);
//            //TimedNumber data = DIODMQ.readDevice("Z:ACLTST@I");
//            //System.out.println("Setting ok");
//
//            System.out.println("DIODMQ setup");
//
//            // Read in IOTA devices
//            Path filePath = new File("devices.txt").toPath();
//            Charset charset = Charset.defaultCharset();
//            List<String> stringList = Files.readAllLines(filePath, charset);
//            devices = new HashSet<>(stringList);
//
//            // Read in IOTA devices
//            Path filePath2 = new File("bpms.txt").toPath();
//            Charset charset2 = Charset.defaultCharset();
//            List<String> stringList2 = Files.readAllLines(filePath2, charset2);
//            bpms = new HashSet<>(stringList2);
//
//            CACHE = new ConcurrentHashMap(devices.size()+bpms.size());
//
//            logger.info(String.format("Starting cache jobs with %d entries", devices.size()+bpms.size()));
//            startDaqJob(stringList, "devices");
//            //startDaqJob(stringList2, "bpms");
//
////            DaqClient daqClient = new DaqClient();
////            daqClient.setCredentialHandler(new DefaultCredentialHandler());
////            //daqClient.setCredentialHandler(new FakeCredentialHandler());
////            HashSet var3 = new HashSet(Arrays.asList(devs));
////            ReadingJob readingJob = daqClient.createReadingJob(var3);
////            readingJob.addDataCallback(new DataUpdateCallback(returnData));
//
//            System.out.println("Setting up worker thread pool");
//            //setupRelay();
//
//            System.out.println(">>>READY<<<");
//        } catch(Exception e){
//            System.out.println("OOF");
//            System.out.println(e);
//            e.printStackTrace();
//        }
//    }
//
//    private static void startDaqJob(List<String> devices, String name) {
//        if (daqClient == null) {
//            daqClient = new DaqClient();
//            daqClient.setCredentialHandler(new DefaultCredentialHandler());
//
//        }
//        //Subject s = daqClient.getCredentials();
//        //daqClient.setCredentialHandler(new FakeCredentialHandler());
//        HashSet hs = new HashSet(devices);
//        ReadingJob readingJob = daqClient.createReadingJob(hs);
//        readingJob.addDataCallback(new DataUpdateCallback(name));
//        System.out.println(daqClient.getCredentials().toString());
//    }
//
//    private static void setupSunServerRelay() throws IOException {
//        HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
//        server.createContext("/test", new MyHandler());
//        server.setExecutor(Executors.newWorkStealingPool());
//        server.start();
//    }
//
//    private static void setupUndertowRelay() throws IOException {
//        Undertow.Builder server = Undertow.builder();
//        server.addHttpListener(8001, "localhost");
//        server.setHandler(new AllowedMethodsHandler(new UndertowHandler(), new HttpString("POST")));
//        server.build().start();
//    }
//}
//
//class FakeCredentialHandler implements CredentialHandler {
//    public final synchronized void credentialException(JobManager var1, GSSException var2) {
//        System.out.println("Cred call");
//        Subject s = new Subject();
//        s.getPrincipals().add(new KerberosPrincipal("FAKE@FNAL.GOV"));
//        var1.setCredentials(s);
//        return;
//    };
//}
//
//class DataUpdateCallback extends TimedNumberCallback {
//
//    ConcurrentHashMap<String, TimedNumber> results;
//    ConcurrentHashMap<String, String> results_json;
//    int num_updates = 0;
//    int num_errors_dev = 0;
//    int num_errors_bpm = 0;
//    int num_errors = 0;
//    String name;
//    ObjectMapper objectMapper;
//
//    public DataUpdateCallback(String name) {
//        this.results = Adapter.CACHE;
//        this.results_json = new ConcurrentHashMap<>();
//        this.name = name;
//        this.objectMapper = new ObjectMapper();
//    }
//
//    public void dataChanged(String var1, TimedNumber var2) {
//        //System.out.println("UPDATE!");
//        num_updates++;
//        if (num_updates % 10000 == 0) {
//            System.out.println(String.format("%s | callbacks: %07d | ERRS -> dev %05d  : bpm %05d  : other %05d ", this.name, num_updates,num_errors_dev,num_errors_bpm,num_errors));
//        }
//        //if (var2.toString().lastIndexOf("72 1") != 1) {
//            //System.out.println(var1);
//            //System.out.println(var2);
//            if (var2 instanceof TimedDouble) {
//                results_json.put(var1, Double.toString(var2.doubleValue()));
//            } else if (var2 instanceof TimedDoubleArray) {
//                results_json.put(var1, Arrays.toString(((TimedDoubleArray) var2).doubleArray()));
//            } else if (var2 instanceof TimedBasicStatus) {
//                results_json.put(var1, var2.toString());
//            } else if (var2 instanceof TimedError) {
//                if (Adapter.devices.contains(var1)) {
//                    num_errors_dev++;
//                } else if (Adapter.bpms.contains(var1)){
//                    num_errors_bpm++;
//                } else {
//                    if (((TimedError) var2).getErrorNumber() != 1) {
//                        num_errors++;
//                    }
//                }
//                int error = ((TimedError) var2).getErrorNumber();
//                if ((error == -89) || (error == -107) || (error == 1) || (error == 3))  {
//                    return; // These are common errors
//                }
//                System.out.println(((TimedError) var2).getErrorNumber() + " | " + var2 + " | " + var1);
//                results_json.put(var1, var2.toString());
//            } else {
//                System.out.println(var2.getClass());
//                System.out.println(var2);
//                results_json.put(var1, "?");
//            }
//            results.put(var1, var2);
//       // }
//    }
//}
//
//class UndertowHandler implements io.undertow.server.HttpHandler {
//
//    ConcurrentHashMap<String, TimedNumber> results = null;
//    ObjectMapper objectMapper = null;
//
//    public UndertowHandler() {
//        this.results = Adapter.CACHE;
//        objectMapper = new ObjectMapper();
//    }
//
//    @Override
//    public void handleRequest(final HttpServerExchange exchange) throws Exception {
//        //System.out.println("REQUEST INCOMING");
//        if (exchange.isInIoThread()) {
//            exchange.dispatch(this);
//            return;
//        }
//
//        InputStream is = exchange.getInputStream();
//        ByteArrayOutputStream result = new ByteArrayOutputStream();
//        byte[] buffer = new byte[1024];
//        int length;
//        while ((length = is.read(buffer)) != -1) {
//            result.write(buffer, 0, length);
//        }
//        String message = result.toString(StandardCharsets.UTF_8.name());
//        System.out.println(">Incoming request:" + result);
//
//        long t1 = System.nanoTime();
//
//        Message r;
//        try {
//            r = this.objectMapper.readValue(message, Message.class);
//        } catch (Exception ex) {
//            ex.printStackTrace();
//            abort("MALFORMED JSON OBJECT", exchange);
//            return;
//        }
//        String requestType = r.requestType;
//        String requestDRF = r.request;
//        //System.out.println(">>T: " + requestType + " | M: " + requestDRF);
//        if (requestDRF == null || requestType == null) {
//            abort("MALFORMED JSON OBJECT", exchange);
//            return;
//        }
//        //checkRequest(requestDRF);
//
//        TimedNumber data = null;
//        HashMap<String, TimedNumber> obj_map = null;
//        try {
//            if (requestType.equalsIgnoreCase("V1_DRF2_READ_MULTI_CACHED") || requestType.equalsIgnoreCase("V1_DRF2_READ_SINGLE_CACHED")) {
//                String[] requests = requestDRF.split(";");
//                obj_map = new HashMap<>(requests.length);
//                for (String req : requests) {
//                    TimedNumber response = null;
//                    if (results.containsKey(req)) {
//                        data = results.get(req);
//                    } else {
//                        abort("INVALID REQUEST - DEVICE NOT IN CACHE", exchange);
//                        return;
//                    }
//                    results.put(req, data);
//                    obj_map.put(req, data);
//                }
//                r.responseTime = Instant.now().toString();
//                //System.out.println(String.format("> Complete! (WITH CACHE)"));
//            } else if (requestType.equalsIgnoreCase("V1_DRF2_READ_SINGLE")) {
//                obj_map = new HashMap<>(1);
//                long startTime = System.nanoTime();
//                data = DIODMQ.readDevice(requestDRF);
//                long endTime = System.nanoTime();
//                long duration = (endTime - startTime);
//                System.out.println(String.format("> Complete! (%.4f ms)", duration / 1e6));
//
//                if (data == null) {
//                    abort("INVALID REQUEST - REJECTED BY ACNET", exchange);
//                    return;
//                }
//                results.put(requestDRF, data);
//                obj_map.put(requestDRF, data);
//                r.responseTime = Instant.now().toString();
//            }
//            HashMap<String, String> map = new HashMap<>(obj_map.size());
//            obj_map.forEach((k, v) -> {
//                String json_string;
//                if (v instanceof TimedError) {
//                    json_string = v.toString();
//                    //json_string = new double[] {(double) ((TimedError) data).getErrorNumber()};
//                    //System.out.println("> ERROR: " + data.toString());
//                } else if (v instanceof TimedDouble) {
//                    json_string = Double.toString(v.doubleValue());
//                    //System.out.println("> DOUBLE: " + data.toString());
//                } else if (v instanceof TimedBasicStatus) {
//                    json_string = v.toString();
//                    //System.out.println("> STATUS: " + data.toString());
//                } else if (v instanceof TimedDoubleArray) {
//                    //json_string = Arrays.toString(((TimedDoubleArray) v).doubleArray());
//                    double[] doubleArray = ((TimedDoubleArray) v).doubleArray();
//                    ByteBuffer buf = ByteBuffer.allocate(Double.SIZE / Byte.SIZE * doubleArray.length);
//                    buf.asDoubleBuffer().put(doubleArray);
//                    json_string = Base64.getEncoder().encodeToString(buf.array());
//                    //System.out.println("> ARRAY: " + Arrays.copyOfRange(((TimedDoubleArray) data).doubleArray(), 0, 10).toString() + " " + data.getUnit() + " " + data.getTimeAsString());
//                } else {
//                    System.out.println(v.getClass());
//                    System.out.println(v);
//                    json_string = "?";
//                }
//                map.put(k, json_string);
//            });
//            r.response = map;
//        } catch (Exception e) {
//            e.printStackTrace();
//            abort("INTERNAL FAILURE: " + e.getMessage(), exchange);
//            System.exit(1);
//        }
//        try {
//            //System.out.println("Writing json");
//            //String response = objectMapper.writeValueAsString(r);
//            //int len = response.length;
//
//            System.out.println(String.format("Time to process (ms): %f | Length: ?", (System.nanoTime() - t1) / 1e6));
//
//            exchange.startBlocking();
//            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
//            exchange.setStatusCode(200);
//            objectMapper.writeValue(exchange.getOutputStream(),r);
//        } catch (Exception e) {
//            e.printStackTrace();
//            abort("INTERNAL FAILURE: " + e.getMessage(), exchange);
//            System.exit(1);
//        } finally {
//            exchange.endExchange();
//        }
//    }
//
//    void abort(String response, final HttpServerExchange exchange) throws IOException {
//        System.err.println("Aborting! " + response);
//        exchange.startBlocking();
//        exchange.setStatusCode(403);
//        exchange.getResponseSender().send(response);
//        exchange.endExchange();
//        return;
//    }
//}
//
//class MyHandler implements HttpHandler {
//
//    ConcurrentHashMap<String, TimedNumber> results = null;
//    Gson gson = null;
//    ObjectMapper objectMapper = null;
//    DslJson<Object> dslJson = null;
//    JsonWriter writer = null;
//
//    public MyHandler() {
//        this.results = Adapter.CACHE;
//        objectMapper = new ObjectMapper();
//        DecimalFormat df = new DecimalFormat("#.######");
//        df.setRoundingMode(RoundingMode.CEILING);
//        GsonBuilder gsonBuilder = new GsonBuilder();
//        gsonBuilder.registerTypeAdapter(Double.class,  new JsonSerializer<Double>() {
//            @Override
//            public JsonElement serialize(final Double src, final Type typeOfSrc, final JsonSerializationContext context) {
//                return new JsonPrimitive(Double.parseDouble(df.format(src)));
//            }
//        });
//        this.gson = gsonBuilder.create();
//        dslJson = new DslJson<Object>(Settings.withRuntime().allowArrayFormat(true).includeServiceLoader());
//        writer = dslJson.newWriter();
//    }
//
//    @Override
//    public void handle(HttpExchange t) throws IOException {
//        //System.out.println("REQUEST INCOMING");
//
//        if (!t.getRequestMethod().equalsIgnoreCase("POST")){
//            abort("ONLY POST REQUESTS ACCEPTED", t);
//        }
//        InputStream is = t.getRequestBody();
//        // JAVA 9+ REEEEE
//        //String result = new String(is.readAllBytes(), StandardCharsets.UTF_8);
//        ByteArrayOutputStream result = new ByteArrayOutputStream();
//        byte[] buffer = new byte[1024];
//        int length;
//        while ((length = is.read(buffer)) != -1) {
//            result.write(buffer, 0, length);
//        }
//        String message = result.toString(StandardCharsets.UTF_8.name());
//        System.out.println(">Incoming request:" + result);
//        long t1 = System.nanoTime();
//
//        Message r;
//        try {
//            r = this.gson.fromJson(message, Message.class);
//        } catch (Exception ex) {
//            //ex.printStackTrace();
//            abort("MALFORMED JSON OBJECT", t);
//            return;
//        }
//        String requestType = r.requestType;
//        String requestDRF = r.request;
//        //System.out.println(">>T: " + requestType + " | M: " + requestDRF);
//        if (requestDRF == null || requestType == null) {
//            abort("MALFORMED JSON OBJECT", t);
//            return;
//        }
//        checkRequest(requestDRF);
//
//        TimedNumber data = null;
//        HashMap<String, TimedNumber> obj_map = null;
//        try {
//            if (requestType.equalsIgnoreCase("V1_DRF2_READ_MULTI_CACHED") || requestType.equalsIgnoreCase("V1_DRF2_READ_SINGLE_CACHED")) {
//                String[] requests = requestDRF.split(";");
//                obj_map = new HashMap<>(requests.length);
//                for (String req : requests) {
//                    TimedNumber response = null;
//                    if (results.containsKey(req)) {
//                        data = results.get(req);
//                    } else {
//                        abort("INVALID REQUEST - DEVICE NOT IN CACHE", t);
//                        return;
//                    }
//                    results.put(req, data);
//                    obj_map.put(req, data);
//                }
//                r.responseTime = Instant.now().toString();
//                //System.out.println(String.format("> Complete! (WITH CACHE)"));
//            } else if (requestType.equalsIgnoreCase("V1_DRF2_READ_SINGLE")) {
//                obj_map = new HashMap<>(1);
//                long startTime = System.nanoTime();
//                data = DIODMQ.readDevice(requestDRF);
//                long endTime = System.nanoTime();
//                long duration = (endTime - startTime);
//                System.out.println(String.format("> Complete! (%.4f ms)", duration / 1e6));
//
//                if (data == null) {
//                    abort("INVALID REQUEST - REJECTED BY ACNET", t);
//                    return;
//                }
//                results.put(requestDRF, data);
//                obj_map.put(requestDRF, data);
//                r.responseTime = Instant.now().toString();
//            }
//            HashMap<String, String> map = new HashMap<>(obj_map.size());
//            obj_map.forEach((k,v) -> {
//                String json_string;
//                if (v instanceof TimedError) {
//                    json_string = v.toString();
//                    //json_string = new double[] {(double) ((TimedError) data).getErrorNumber()};
//                    //System.out.println("> ERROR: " + data.toString());
//                } else if (v instanceof TimedDouble) {
//                    json_string = Double.toString(v.doubleValue());
//                    //System.out.println("> DOUBLE: " + data.toString());
//                } else if (v instanceof TimedBasicStatus) {
//                    json_string = v.toString();
//                    //System.out.println("> STATUS: " + data.toString());
//                } else if (v instanceof TimedDoubleArray) {
//                    //json_string = Arrays.toString(((TimedDoubleArray) v).doubleArray());
//                    double[] doubleArray = ((TimedDoubleArray) v).doubleArray();
//                    ByteBuffer buf = ByteBuffer.allocate(Double.SIZE / Byte.SIZE * doubleArray.length);
//                    buf.asDoubleBuffer().put(doubleArray);
//                    json_string = Base64.getEncoder().encodeToString(buf.array());
//                    //System.out.println("> ARRAY: " + Arrays.copyOfRange(((TimedDoubleArray) data).doubleArray(), 0, 10).toString() + " " + data.getUnit() + " " + data.getTimeAsString());
//                } else {
//                    System.out.println(v.getClass());
//                    System.out.println(v);
//                    json_string = "?";
//                }
//                map.put(k, json_string);
//            });
//            r.response = map;
//        } catch (Exception e) {
//            e.printStackTrace();
//            abort("INTERNAL FAILURE: " + e.getMessage() , t);
//            System.exit(1);
//        }
//
//        OutputStream os = t.getResponseBody();
//        try {
//            //System.out.println("Writing json");
//
//            String response = objectMapper.writeValueAsString(r);
//            //String response = this.gson.toJson(r);
//            byte[] bytes = response.getBytes();
//            int len = response.length();
//
////            DslJson<Object> dslJson2 = new DslJson<>(Settings.withRuntime());
////            JsonWriter writer2 = dslJson2.newWriter();
////            dslJson2.serialize(writer2, r);
////            byte[] bytes = writer2.getByteBuffer();
////            int len = bytes.length;
//
//            System.out.println(String.format("Time to process (ms): %f | Length: %d",(System.nanoTime() - t1)/1e6, len));
//            t.sendResponseHeaders(200, len);
//            os.write(bytes);
//        } catch (Exception e) {
//            e.printStackTrace();
//            abort("INTERNAL FAILURE: " + e.getMessage() , t);
//            System.exit(1);
//        } finally {
//            os.close();
//        }
//    }
//
//    void checkRequest(String requestDRF) {
//        if (//requestDRF.contains("[") ||
//            //requestDRF.contains("]") ||
//            !(requestDRF.contains(":") || requestDRF.contains("|") || requestDRF.contains("_")) ||
//            !Charset.forName(StandardCharsets.US_ASCII.name()).newEncoder().canEncode(requestDRF)) {
//            //abort("INVALID REQUEST - REJECTED BY SANITY CHECKS", t);
//        }
//    }
//
//    void abort(String response, HttpExchange t) throws IOException {
//        System.err.println("Aborting! " + response);
//        t.sendResponseHeaders(403, response.length());
//        OutputStream os = t.getResponseBody();
//        os.write(response.getBytes());
//        os.close();
//        return;
//    }
//}