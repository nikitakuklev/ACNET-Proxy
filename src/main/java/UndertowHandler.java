import com.fasterxml.jackson.databind.ObjectMapper;
import gov.fnal.controls.service.proto.DAQData;
import gov.fnal.controls.tools.dio.DIODMQ;
import gov.fnal.controls.tools.timed.*;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UndertowHandler implements io.undertow.server.HttpHandler {

    ObjectMapper objectMapper;
    private static final Logger logger = Logger.getLogger(Adapter2.class.getName());

    public UndertowHandler() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        long t0 = System.nanoTime();
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }
        if (!exchange.getRequestPath().equals("/")) {
            abort("Wrong path!", exchange);
            return;
        }
        long t1 = System.nanoTime();
        //logger.fine(">In blocking thread:" + (t1-t0)/1e3);

        exchange.startBlocking();
        InputStream is = exchange.getInputStream();
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = is.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        String message = result.toString(StandardCharsets.UTF_8.name());

        if (exchange.getRequestMethod().equals(new HttpString("GET"))) {
            try {
                System.out.println("GET request - returning debug info");
                String response = objectMapper.writeValueAsString(Adapter2.callbacks.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().errors)));
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                exchange.setStatusCode(200);
                exchange.getResponseSender().send(response);
            } catch (Exception e) {
                e.printStackTrace();
                abort("INTERNAL SERIALIZATION FAILURE: " + e.getMessage(), exchange);
                System.exit(1);
            } finally {
                System.out.println(String.format("Total time (ms): %f | Length: ?", (System.nanoTime() - t0) / 1e6));
                exchange.endExchange();

            }
            return;
        }

        long t2 = System.nanoTime();
        //System.out.println(">Request read:" + message + ' ' + (t2-t1)/1e3);

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
        //System.out.println(String.format("> %f - Request parsed: %s %s ", (t3-t2)/1e3,requestType,requestDRF));

        if (!checkRequest(requestDRF)) {abort("INVALID REQUEST - REJECTED BY SANITY CHECKS", exchange);};

        TimedNumber data = null;
        HashMap<String, TimedNumber> obj_map;
        long t4 = 0;
        try {
            if (requestType.equalsIgnoreCase("V1_DRF2_READ_MULTI_CACHED")) {
                // This type returns latest value in cache, if any
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
                        //System.out.println(DRFCache.CACHE.keySet());
                        abort(String.format("INVALID REQUEST - DEVICE %s NOT IN CACHE",req), exchange);
                        return;
                    } else {
                        obj_map.put(req, data);
                    }
                }
                //System.out.println(String.format("> Complete! (WITH CACHE)"));
            } else if (requestType.equalsIgnoreCase("V1_DRF2_READ_SINGLE")) {
                // This type returns latest value in cache, if any
                String[] requests = requestDRF.split(";");
                obj_map = new HashMap<>(requests.length);
                HashMap<String, TimedNumber> results = DIODMQ.readDeviceList(requests);
                for (Map.Entry<String,TimedNumber> entry : results.entrySet()) {
                    data = entry.getValue();
                    if (data == null) {
                        abort(String.format("INVALID REQUEST - %s REJECTED BY ACNET",entry.getKey()), exchange);
                        return;
                    } else {
                        obj_map.put(entry.getKey(), data);
                    }
                }
                //System.out.println(String.format("> Complete! (WITH CACHE)"));
//                obj_map = new HashMap<>(1);
//                long startTime = System.nanoTime();
//                data = DIODMQ.readDevice(requestDRF);
//                long duration = (System.nanoTime() - startTime);
//                //System.out.println(String.format(">(%s) complete! (%.4f ms)", requestType, duration / 1e6));
//
//                if (data == null) {
//                    abort("INVALID REQUEST - REJECTED BY ACNET", exchange);
//                    return;
//                }
//                obj_map.put(requestDRF, data);
            } else if (requestType.equalsIgnoreCase("V1_DRF2_SET_SINGLE")) {
                // This method should work for all relevant settings, since we can use
                // canonical DRF2 (i.e. STATUS.ON). Only restriction is reserved status keywords.
                String requestDRFlocal = null;//requestDRF;
                if (requestDRF.contains(".")) {
                    requestDRFlocal = requestDRF.split("\\.")[0];
                    //System.out.println(String.format("Reading %s formatted: %s", requestDRF, requestDRFlocal));
                }
                obj_map = new HashMap<>(1);
                long startTime = System.nanoTime();
                boolean null_override = false;
                for (DAQData.BasicControl b : DAQData.BasicControl.values()) {
                    if (b.name().equalsIgnoreCase(r.requestValue)) {
                        TimedBasicControl timed = new TimedBasicControl(b);
                        DAQData.Reply reply = TimedNumberFactory.toProto(timed);
                        if (Adapter2.setters.contains(requestDRF)) {
                            //System.out.println(String.format("CACHED status setting %s",requestDRF));
                            Adapter2.settingJob.setData(requestDRF, reply);
                            data = ((SettingCallback)Adapter2.settingCallback).getData();
                        } else {
                            data = DIODMQ.setDevice(requestDRF,  b);
                        }
                    }
                }

                if (data == null) {
                    try {
                        double val = Double.parseDouble(r.requestValue);
                        if (Adapter2.setters.contains(requestDRF)) {
                            //System.out.println(String.format("CACHED double setting %s",requestDRF));
                            TimedDouble td = new TimedDouble(val);
                            DAQData.Reply reply = TimedNumberFactory.toProto(td);
                            Adapter2.settingJob.setData(requestDRF, reply);
                            data = ((SettingCallback)Adapter2.settingCallback).getData();
                        } else {
                            data = DIODMQ.setDevice(requestDRF, val);
                        }
                    } catch (NumberFormatException e) {
                        data = DIODMQ.setDevice(requestDRF, r.requestValue);
                    }
                }
                //long duration = (System.nanoTime() - startTime);
                //System.out.println(String.format(">(%s) complete! (%.4f ms) (%s)", requestType, duration / 1e6, requestDRF));
                if (data == null && !null_override) {
                    abort("INVALID REQUEST - REJECTED BY ACNET", exchange);
                    return;
                }
                obj_map.put(requestDRF, data);

            } else if (requestType.equalsIgnoreCase("V1_DRF2_SET_MULTI")) {
                String[] requests = requestDRF.split(";");
                String[] values = r.requestValue.split(";");
                //DAQData.BasicControl[] basics = Arrays.stream(DAQData.BasicControl.values()).;

                obj_map = new HashMap<>(requests.length);
                for (int i=0; i<requests.length; i++) {
                    data = null;
                    String req = requests[i];
                    String value = values[i];
                    long startTime = System.nanoTime();
                    try {
                        double val = Double.parseDouble(value);
                        if (Adapter2.setters.contains(req)) {
                            //System.out.println(String.format("CACHED double setting %s",req));
                            TimedDouble td = new TimedDouble(val);
                            DAQData.Reply reply = TimedNumberFactory.toProto(td);
                            Adapter2.settingJob.setData(req, reply);
                            data = ((SettingCallback)Adapter2.settingCallback).getData();
                        } else {
                            System.out.println(String.format("NONCH double setting %s",req));
                            data = DIODMQ.setDevice(req, val);
                        }
                    } catch (NumberFormatException e) {
//                        for (DAQData.BasicControl b : DAQData.BasicControl.values()) {
//                            if (b.name().equalsIgnoreCase(value)) {
//                                TimedBasicControl timed = new TimedBasicControl(b);
//                                DAQData.Reply reply = TimedNumberFactory.toProto(timed);
//                                if (Adapter2.setters.contains(req)) {
//                                    System.out.println(String.format("CACHED status setting %s",req));
//                                    Adapter2.settingJob.setData(req, reply);
//                                    data = ((SettingCallback)Adapter2.settingCallback).getData();
//                                } else {
//                                    System.out.println(String.format("NONCH status setting %s",req));
//                                    data = DIODMQ.setDevice(req,  b);
//                                }
//                                break;
//                            }
//                        }
                        System.out.println(String.format("NONCH string setting %s",req));
                        data = DIODMQ.setDevice(req, value);
                    }
                    if (data == null) {
                        abort("INVALID REQUEST - REJECTED BY ACNET", exchange);
                        return;
                    }
                    long duration = (System.nanoTime() - startTime);
                    System.out.println(String.format(">Set device %d/%d! (%.4f ms)", i+1, requests.length, duration / 1e6));
                    obj_map.put(req, data);
                    Thread.sleep(10);
                }
            } else {
                abort("INVALID REQUEST TYPE", exchange);
                return;
            }
            r.responseTime = Instant.now().toString();

            t4 = System.nanoTime();
            //System.out.println(">Request processed:" + (t4-t3)/1e3);

            HashMap<String, String> map = new HashMap<>(obj_map.size());
            obj_map.forEach((k, v) -> map.put(k, convertTimedNumber(v)));
            r.responseJson = map;
        } catch (Exception e) {
            e.printStackTrace();
            abort(requestDRF + " - INTERNAL FAILURE: " + e.getMessage(), exchange);
            System.exit(1);
        }

        long t5 = System.nanoTime();
        //System.out.println(">Request serialized:" + (t5-t4)/1e3);

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
            System.out.println(String.format("Req (%s) - (tot|proc) (%.4f|%.4f), drf: %s, %s",
                    requestType, (System.nanoTime() - t0) / 1e6, (t4-t3)/1e6, requestDRF, r.requestValue));
            exchange.endExchange();
        }
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
        } else if (v instanceof TimedBoolean) {
            return v.toString();
        } else if (v instanceof TimedError) {
            return v.toString();
        } else {
            System.out.println("Can't stringify:" + v.getClass());
            System.out.println(v);
            throw new IllegalArgumentException();
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
    }
}