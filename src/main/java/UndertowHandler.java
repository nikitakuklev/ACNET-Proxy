import com.fasterxml.jackson.databind.ObjectMapper;
import gov.fnal.controls.acnet.AcnetErrors;
import gov.fnal.controls.daq.acquire.SettingsState;
import gov.fnal.controls.service.dmq.*;
//import gov.fnal.controls.service.dmq.impl.rabbit.ClientSettingJob;
import gov.fnal.controls.service.proto.DAQData;
import gov.fnal.controls.tools.dio.DIODMQ;
import gov.fnal.controls.tools.drf2.DataRequest;
import gov.fnal.controls.tools.drf2.DiscreteRequest;
import gov.fnal.controls.tools.timed.*;
import io.undertow.server.DefaultResponseListener;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

class ErrorHandler implements io.undertow.server.HttpHandler {

    private final HttpHandler next;

    private static final Logger logger = Logger.getLogger(UndertowHandler.class.getName());

    public ErrorHandler(final HttpHandler next) {
        this.next = next;
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {

        try {
            exchange.addDefaultResponseListener(new DefaultResponseListener() {
                @Override
                public boolean handleDefaultResponse(final HttpServerExchange exchange) {
                    if (!exchange.isResponseChannelAvailable()) {
                        return false;
                    }
                    if (exchange.getStatusCode() == 500) {
                        final String errorPage = "Internal Error (from default listener)";
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                        exchange.getResponseSender().send(errorPage);
                        return true;
                    }
                    return false;
                }
            });

            next.handleRequest(exchange);
        } catch (Exception e) {
            logger.warning("Caught exception in error handler: " + e);
            e.printStackTrace();
            if(exchange.isResponseChannelAvailable()) {
                final String errorPage = "Internal Error (from error handler catch block)";
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                exchange.getResponseSender().send(errorPage);
            }
        }
    }
}


public class UndertowHandler implements io.undertow.server.HttpHandler {

    public ObjectMapper objectMapper;
    public static final Logger logger = Logger.getLogger(UndertowHandler.class.getName());

    public static final HashMap<String, DIODMQSettingJobCustom> deviceMap = new HashMap<>();

    public static final HashMap<String, DaqClient> clientMap = new HashMap<>();



    public UndertowHandler() {
        objectMapper = new ObjectMapper();
    }

    public static DIODMQSettingJobCustom getSettingJob(String device) throws JobException, InterruptedException {
        if (Adapter3.daqSettingClient == null) {
            DaqClient daqSettingClient = new DaqClient(Adapter3.preset);
            daqSettingClient.setCredentialHandler(Adapter3.credHandler);
            Thread.sleep(500);
            Adapter3.daqSettingClient = daqSettingClient;
            deviceMap.clear();
            clientMap.clear();
            daqSettingClient.enableSetting(SettingsState.FOREVER, TimeUnit.MINUTES);
        }
        DIODMQSettingJobCustom job = deviceMap.get(device);
        if (job == null) {
            logger.severe(String.format("Making new sjob for [%s]", device));
            DaqClient daqSettingClient = Adapter3.daqSettingClient;
            //DaqClient daqSettingClient = new DaqClient(Adapter3.preset);
            //daqSettingClient.setCredentialHandler(Adapter3.credHandler);
            //daqSettingClient.enableSetting(SettingsState.FOREVER, TimeUnit.MINUTES);
            clientMap.put(device, daqSettingClient);
            Thread.sleep(100);
            job = new DIODMQSettingJobCustom(daqSettingClient, device);
            job.getDataCallback().getData();
            deviceMap.put(device, job);
            //Thread.sleep(200);
            Thread.sleep(10);
        }
//        DaqClient dcl = Adapter3.daqSettingClient;
//        if (!dcl.isSettingEnabled()) {
//            dcl.enableSetting(SettingsState.FOREVER, TimeUnit.MINUTES);
//        }
        return job;
    }

    private static TimedError setDevice(String request, DAQData.Reply proto) {
        TimedError status = new TimedError(0, 0);
        TimedNumber status2;
        try {
            DIODMQSettingJobCustom job = getSettingJob(request);
            DIODMQSettingCallbackCustom settingCallback = job.getDataCallback();
            job.setData(request, proto);
            status2 = settingCallback.getData();
            if (status2 == null) {
                // DO NOT LOOKUP ERROR TEXT OR THINGS CRASH
                status2 = new TimedError(AcnetErrors.FACILITY_DIO, -43,
                        "ARRRRR ACNET BAD ARRRRR", System.currentTimeMillis());
                if (deviceMap.containsKey(request)) {
                    logger.warning(String.format("SET TIMEOUT - REMOVING JOB FOR [%s]", request));
                    deviceMap.remove(request);
                }
                DaqClient cl = clientMap.get(request);
                cl.dispose();
                //job.getSettingJob().dispose();
                clientMap.remove(request);
                Adapter3.daqSettingClient = null;
                deviceMap.clear();
                clientMap.clear();
            }
            if (status2 instanceof TimedError) {
                status = (TimedError) status2;
            }
            return status;
        } catch (JobException je) {
            logger.warning("Error setting device " + request + ": "  + je.getMessage());
            status = new TimedError(je.getFacilityCode(), je.getErrorNumber());
            return status;
        } catch (InterruptedException e) {
            logger.severe("THREAD INTERRUPT NOT SUPPOSED TO HAPPEN");
            //do nothing
            return null;
        }
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        try {
            long t0 = System.nanoTime();
            if (exchange.getRequestPath().equals("/status")) {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                exchange.setStatusCode(200);
                exchange.getResponseSender().send("OK");
                exchange.endExchange();
                return;
            }
            if (!exchange.getRequestPath().equals("/")) {
                abort(String.format("Wrong path abort: %s", exchange.getRequestPath()), exchange);
                return;
            }

            // This will be a long request -> move to worker
            if (exchange.isInIoThread()) {
                exchange.dispatch(this);
                return;
            }
            long t1 = System.nanoTime();
            //logger.fine(">In blocking thread:" + (t1-t0)/1e3);

            exchange.startBlocking();
            if (!exchange.isResponseChannelAvailable()) {
                logger.severe("GOT ENDED EXCHANGE IN WORKER, RETURNING EARLY");
                exchange.endExchange();
                return;
            }
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
                    String response = objectMapper.writeValueAsString(Adapter3.callbacks.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().errors)));
                    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                    exchange.setStatusCode(200);
                    exchange.getResponseSender().send(response);
                } catch (Exception e) {
                    e.printStackTrace();
                    abort("INTERNAL SERIALIZATION FAILURE: " + e.getMessage(), exchange);
                    System.exit(1);
                } finally {
                    System.out.printf("Total time (ms): %f | Length: ?%n", (System.nanoTime() - t0) / 1e6);
                    exchange.endExchange();
                }
                return;
            }

            //long t2 = System.nanoTime();
            //System.out.println(">Request read:" + message + ' ' + (t2-t1)/1e3);

            Message r;
            try {
                r = this.objectMapper.readValue(message, Message.class);
            } catch (Exception ex) {
                ex.printStackTrace();
                abort("MALFORMED JSON OBJECT", exchange);
                logger.severe("MALFORMED JSON OBJECT");
                logger.severe(traceToString(ex));
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

            if (!checkRequest(requestDRF)) {
                abort("INVALID REQUEST - REJECTED BY SANITY CHECKS", exchange);
                return;
            }

            HashMap<String, TimedNumber> obj_map;
            long t4 = 0;
            try {
                if (requestType.equalsIgnoreCase("READ_CACHED")) {
                    // This type returns latest value in cache, if any
                    TimedNumber data = null;
                    String[] requests = requestDRF.split(";");
                    obj_map = new HashMap<>(requests.length);
                    for (String req : requests) {
                        DiscreteRequest req_parsed = (DiscreteRequest) DataRequest.parse(req);
                        String device_name = req_parsed.getDevice();
                        String a = DRFCache.NAMEMAP.getOrDefault(device_name, null);
                        if (a == null) {
                            data = DRFCache.CACHE.getOrDefault(req, null);
                            r.response = req;
                        } else {
                            data = DRFCache.CACHE.getOrDefault(a, null);
                            r.response = a;
                        }
                        if (data == null) {
                            abort(String.format("INVALID REQUEST - DEVICE (%s) NOT IN CACHE", device_name), exchange);
                            return;
                        } else {
                            obj_map.put(req, data);
                        }
                    }
                    //data_final = data;
                    //System.out.println(String.format("> Complete! (WITH CACHE)"));
                } else if (requestType.equalsIgnoreCase("READ")) {
                    // Read fresh data
                    TimedNumber data;
                    String[] requests = requestDRF.split(";");
                    obj_map = new HashMap<>(requests.length);
                    logger.info(String.format("READ START [%d devices]", requests.length));
                    HashMap<String, TimedNumber> results = readDeviceList(requests);
                    if (results == null) {
                        abort("Read timed out", exchange);
                        return;
                    }
                    int i = 0;
                    for (Map.Entry<String, TimedNumber> entry : results.entrySet()) {
                        data = entry.getValue();
                        if (data instanceof TimedArray) {
                            logger.info(String.format(" - Read array %03d/%03d [%s] -> [%s]", i + 1, requests.length, entry.getKey(),
                                    data.doubleValue()));
                        } else {
                            logger.info(String.format(" - Read %03d/%03d [%s] -> [%s]", i + 1, requests.length, entry.getKey(),
                                    data));
                        }

                        if (data == null) {
                            abort(String.format("INVALID REQUEST - %s REJECTED BY ACNET", entry.getKey()), exchange);
                            return;
                        } else {
                            obj_map.put(entry.getKey(), data);
                        }
                        i += 1;
                    }
                    //long duration = (System.nanoTime() - startTime);
                }
//            else if (requestType.equalsIgnoreCase("V1_DRF2_SET_SINGLE")) {
//                // This method should work for all relevant settings, since we can use
//                // canonical DRF2 (i.e. STATUS.ON). Only restriction is reserved status keywords.
//                String requestDRFlocal = null;//requestDRF;
//                if (requestDRF.contains(".")) {
//                    requestDRFlocal = requestDRF.split("\\.")[0];
//                    //System.out.println(String.format("Reading %s formatted: %s", requestDRF, requestDRFlocal));
//                }
//                obj_map = new HashMap<>(1);
//                long startTime = System.nanoTime();
//                boolean null_override = false;
//                for (DAQData.BasicControl b : DAQData.BasicControl.values()) {
//                    if (b.name().equalsIgnoreCase(r.requestValue)) {
//                        TimedBasicControl timed = new TimedBasicControl(b);
//                        DAQData.Reply reply = TimedNumberFactory.toProto(timed);
//                        if (Adapter3.setters.contains(requestDRF)) {
//                            //System.out.println(String.format("CACHED status setting %s",requestDRF));
//                            Adapter3.settingJob.setData(requestDRF, reply);
//                            data = ((SettingCallback)Adapter3.settingCallback).getData();
//                        } else {
//                            data = DIODMQ.setDevice(requestDRF,  b);
//                        }
//                    }
//                }
//
//                if (data == null) {
//                    try {
//                        double val = Double.parseDouble(r.requestValue);
//                        if (Adapter3.setters.contains(requestDRF)) {
//                            //System.out.println(String.format("CACHED double setting %s",requestDRF));
//                            TimedDouble td = new TimedDouble(val);
//                            DAQData.Reply reply = TimedNumberFactory.toProto(td);
//                            Adapter3.settingJob.setData(requestDRF, reply);
//                            data = ((SettingCallback)Adapter3.settingCallback).getData();
//                        } else {
//                            data = DIODMQ.setDevice(requestDRF, val);
//                        }
//                    } catch (NumberFormatException e) {
//                        data = DIODMQ.setDevice(requestDRF, r.requestValue);
//                    }
//                }
//                //long duration = (System.nanoTime() - startTime);
//                //System.out.println(String.format(">(%s) complete! (%.4f ms) (%s)", requestType, duration / 1e6, requestDRF));
//                if (data == null && !null_override) {
//                    abort("INVALID REQUEST - REJECTED BY ACNET", exchange);
//                    return;
//                }
//                obj_map.put(requestDRF, data);
//
//            }
                else if (requestType.equalsIgnoreCase("SET_MULTI")) {
                    String[] requests = requestDRF.split(";");
                    String[] values = r.requestValue.split(";");

                    obj_map = new HashMap<>(requests.length);
                    LinkedHashMap<String, SettingJob> cached_devices = new LinkedHashMap<>(128);
                    for (int i = 0; i < requests.length; i++) {
                        String req = requests[i];
                        String value = values[i];
                        long startTime = System.nanoTime();
                        Thread.sleep(10);

                        boolean isdbl = false;
                        boolean iscontrol = false;
                        try {
                            Double.parseDouble(value);
                            isdbl = true;
                        } catch (NumberFormatException ignored) {
                            for (DAQData.BasicControl b : DAQData.BasicControl.values()) {
                                if (b.name().equalsIgnoreCase(value)) {
                                    iscontrol = true;
                                    break;
                                }
                            }
                        }
                        if (isdbl) {
                            double val = Double.parseDouble(value);
                            TimedDouble td = new TimedDouble(val);
                            DAQData.Reply reply = TimedNumberFactory.toProto(td);
                            boolean done = false;
                            for (int j = 0; j < Adapter3.settersList.size(); j++) {
                                HashSet<String> devices = Adapter3.settersList.get(j);
                                if (devices.contains(req)) {
                                    SettingJob sj = Adapter3.settingJobs.get(j);
                                    DaqClient dcl = Adapter3.daqSettingClients.get(j);
                                    if (!dcl.isSettingEnabled()) {
                                        dcl.enableSetting(SettingsState.FOREVER, TimeUnit.MINUTES);
                                    }
                                    SettingCallback cb = Adapter3.settingCallbackMap.get(sj);
                                    cb.clearData(req);
                                    //long i1 = sj.getSettingCount();
                                    sj.setData(req, reply);
//                                    if (sj.getSettingCount() != i1 + 1) {
//                                        abort(String.format("SETTING JOB FAILED - NULL DATA FOR %s", req), exchange);
//                                        return;
//                                    }
                                    cached_devices.put(req, sj);
                                    logger.info(String.format("Set CACHED double (%03d, j%d) [%s] -> [%04.5f]", i, j, req, val));
                                    done = true;
                                    break;
                                }
                            }
                            if (!done) {
                                logger.info(String.format("Set UNCACHED double [%s] -> [%s]", req, val));
                                TimedError data = setDevice(req, reply);
                                TimedError data_final = checkData(req, data);
                                if (data_final == null) {
                                    abort(String.format("ERROR - NULL DATA FOR %s", req), exchange);
                                    return;
                                }
                                obj_map.put(req, data_final);
                            }
                        }
//                    else if (iscontrol) {
//                        for (DAQData.BasicControl b : DAQData.BasicControl.values()) {
//                            if (b.name().equalsIgnoreCase(value)) {
//                                TimedBasicControl timed = new TimedBasicControl(b);
//                                DAQData.Reply reply = TimedNumberFactory.toProto(timed);
//                                if (Adapter3.setters.contains(req)) {
//                                    System.out.println(String.format("CACHED status setting %s",req));
//                                    Adapter3.settingJob.setData(req, reply);
//                                    data = ((SettingCallback)Adapter3.settingCallback).getData();
//                                } else {
//                                    System.out.println(String.format("NONCH status setting %s",req));
//                                    data = DIODMQ.setDevice(req,  b);
//                                }
//                                break;
//                            }
//                        }
//                        logger.info(String.format("NON-CACHED string setting %s", req));
//                        TimedNumber data = DIODMQ.setDevice(req, value);
//                        TimedError data_final = checkData(req, data);
//                        if (data_final == null) {
//                            abort(String.format("ERROR - NULL DATA FOR %s", req), exchange);
//                            return;
//                        }
//                        obj_map.put(req, data_final);
//                    }
                        else {
                            boolean done = false;
                            TimedString td = new TimedString(value);
                            DAQData.Reply reply = TimedNumberFactory.toProto(td);
                            for (int j = 0; j < Adapter3.settersList.size(); j++) {
                                HashSet<String> devices = Adapter3.settersList.get(j);
                                if (devices.contains(req)) {
                                    SettingJob sj = Adapter3.settingJobs.get(j);
                                    DaqClient dcl = Adapter3.daqSettingClients.get(j);
                                    if (!dcl.isSettingEnabled()) {
                                        dcl.enableSetting(SettingsState.FOREVER, TimeUnit.MINUTES);
                                    }
                                    SettingCallback cb = Adapter3.settingCallbackMap.get(sj);
                                    cb.clearData(req);
                                    sj.setData(req, reply);
                                    cached_devices.put(req, sj);
                                    logger.info(String.format("Set CACHED STR (%03d, j%d) [%s] -> [%s]", i, j, req, value));
                                    done = true;
                                    break;
                                }
                            }
                            if (!done) {
                                logger.info(String.format("Set UNCACHED STR [%s] -> [%s]", req, value));
                                TimedError data = setDevice(req, reply);
                                TimedError data_final = checkData(req, data);
                                if (data_final == null) {
                                    abort(String.format("ERROR - NULL DATA FOR %s", req), exchange);
                                    return;
                                }
                                obj_map.put(req, data_final);
                            }
                        }
                        long duration = (System.nanoTime() - startTime);
                        logger.info(String.format(" | Set %03d/%03d (%.3f ms)%n", i + 1, requests.length, duration / 1e6));
                        //Thread.sleep(1);
                    }
                    // Get async results
                    long start_poll_time = System.currentTimeMillis();
                    for (String req : cached_devices.keySet()) {
                        SettingJob sj = cached_devices.get(req);
                        SettingCallback cb = Adapter3.settingCallbackMap.get(sj);
                        TimedNumber data = cb.getDataAsync(req, start_poll_time);
                        logger.info(String.format("SetResult CACHED double (%s) = (%s)%n", req, data));
                        TimedError data_final = checkData(req, data);
                        if (data_final == null) {
                            abort(String.format("ERROR - NULL DATA FOR %s", req), exchange);
                            return;
                        }
                        obj_map.put(req, data_final);
                        //SynchronousQueue<TimedNumber> q = cb.lastValues.get(dev);
                    }
                } else {
                    abort(String.format("INVALID REQUEST TYPE %s", requestType), exchange);
                    return;
                }
                r.responseTime = Instant.now().toString();

                t4 = System.nanoTime();
                //System.out.println(">Request processed:" + (t4-t3)/1e3);

                HashMap<String, HashMap<String, String>> map = new HashMap<>(obj_map.size());
                obj_map.forEach((k, v) -> map.put(k, convertTimedNumber(v)));
                r.responseJson = map;

                HashMap<String, String> map2 = new HashMap<>(obj_map.size());
                obj_map.forEach((k, v) -> map2.put(k, convertTimedNumberTimestamp(v)));
                r.responseTimestampJson = map2;
            } catch (Exception e) {
                e.printStackTrace();
                abort(requestDRF + " - INTERNAL FAILURE: " + e.getMessage() + e.toString(), exchange);
                logger.severe(requestDRF + " - INTERNAL FAILURE: " + e.getMessage() + e.toString());
                logger.severe(traceToString(e));
                System.out.flush();
                System.exit(2);
            }

            long t5 = System.nanoTime();
            //System.out.println(">Request serialized:" + (t5-t4)/1e3);

            try {
                //System.out.println("Writing json");
                //String response = objectMapper.writeValueAsString(r);
                //int len = response.length;
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                exchange.setStatusCode(200);
                OutputStream s = exchange.getOutputStream();
                objectMapper.writeValue(s, r);
            } catch (IOException e) {
                logger.severe("IOexception: " + e.getMessage());
                logger.severe(traceToString(e));
                abort("IOexception: " + e.getMessage(), exchange);
            } catch (Exception e) {
                e.printStackTrace();
                abort("INTERNAL SERIALIZATION FAILURE: " + e.getMessage(), exchange);
                logger.severe("INTERNAL SERIALIZATION FAILURE: " + e.getMessage());
                logger.severe(traceToString(e));
                System.exit(3);
            } finally {
                logger.info(String.format("(%s) done in (tot|proc) (%.4f|%.4f)%n", requestType, (System.nanoTime() - t0) / 1e6, (t4 - t3) / 1e6));
                if (r.requestValue.equals("")) {
                    logger.info(String.format("drf: %s%n", requestDRF));
                } else {
                    logger.info(String.format("drf: %s, values: %s%n", requestDRF, r.requestValue));
                }
                logger.info("================================================");
                logger.info("");
                exchange.endExchange();
            }
        } catch (Exception ex) {
            ex.printStackTrace();

            //abort("FATAL OUTER SERVER LOOP EXCEPTION - TERMINATING PROXY" + ex.getMessage(), exchange);
            logger.severe("FATAL OUTER SERVER LOOP EXCEPTION - TERMINATING PROXY" + ex.getMessage());
            logger.severe(traceToString(ex));
            System.out.flush();
            System.exit(4);
        }
    }

    public String traceToString(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }


    public HashMap<String,TimedNumber> readDeviceList(String[] dataRequestArray ) {
        ReadingJob readingJob;
        Set<String> dataRequest = new HashSet<>(Arrays.asList(dataRequestArray));
        readingJob = Adapter3.daqDefaultClient.createReadingJob(dataRequest);
        DIODMQTimedDataCallback callback = new DIODMQTimedDataCallback(dataRequest);
        readingJob.addDataCallback(callback);
        HashMap<String,TimedNumber> rawData = callback.getData();
        //logger.info(String.format("gss: %s%n", dataRequestArray));
        if (rawData == null) {
            // no data at all received
            //new TimedError(14, -43, AcnetError.getText(-10994), System.currentTimeMillis());
            readingJob.dispose();
            return null;
        } else {
            HashMap<String, TimedNumber> returnData = new HashMap<>(rawData.size());
            for (String s : dataRequest) {
                returnData.put(s, rawData.get(s));
            }
            readingJob.dispose();
            return returnData;
        }
    }

    public TimedError checkData(String req, TimedNumber data) {
        TimedError d;
        if (data instanceof TimedError) {
            d = (TimedError) data;
        } else if (data == null) {
            d = new TimedError(14, -43, "SET DATA TIMEOUT", System.currentTimeMillis());
        } else {
            //AcnetError.getText(-10994)
            d = new TimedError(0, 0);
        }
        int error = d.getErrorNumber();
        int fc = d.getFacilityCode();
        if ((fc != 0) || (error != 0)) {
            System.out.printf("Settings error for (%s), fc %d, err %d, msg %s%n", req, fc, error, d.getMessage());
        }
        return d;
    }

    public static HashMap<String, String> convertTimedNumber(TimedNumber v) {
        if (v instanceof TimedDouble) {
            HashMap<String, String> map = new HashMap<>(1);
            map.put("data", Double.toString(v.doubleValue()));
            map.put("error", "");
            map.put("type", "DOUBLE");
            return map;
        } else if (v instanceof TimedDoubleArray) {
            double[] doubleArray = ((TimedDoubleArray) v).doubleArray();
            ByteBuffer buf = ByteBuffer.allocate(Double.SIZE / Byte.SIZE * doubleArray.length);
            buf.asDoubleBuffer().put(doubleArray);
            HashMap<String, String> map = new HashMap<>(1);
            map.put("data", Base64.getEncoder().encodeToString(buf.array()));
            map.put("error", "");
            map.put("type", "DOUBLEARRAY");
            return map;
        } else if (v instanceof TimedBasicStatus) {
            HashMap<String, String> map = new HashMap<>(5);
            map.put("on", ((TimedBasicStatus) v).isOn().toString());
            map.put("ready", ((TimedBasicStatus) v).isReady().toString());
            map.put("remote", ((TimedBasicStatus) v).isRemote().toString());
            map.put("positive", ((TimedBasicStatus) v).isPositive().toString());
            map.put("ramp", ((TimedBasicStatus) v).isRamp().toString());
            map.put("error", "");
            map.put("type", "BASICSTATUS");
            return map;
        } else if (v instanceof TimedBoolean) {
            HashMap<String, String> map = new HashMap<>(1);
            map.put("data", v.toString());
            map.put("error", "");
            map.put("type", "BOOLEAN");
            return map;
        } else if (v instanceof TimedError) {
            HashMap<String, String> map = new HashMap<>(1);
            map.put("data", "");
            map.put("error", v.toString());
            map.put("facility_code", String.valueOf(((TimedError) v).getFacilityCode()));
            map.put("error_number", String.valueOf(((TimedError) v).getErrorNumber()));
            map.put("error_message", String.valueOf(((TimedError) v).getMessage()));
            map.put("type", "ERROR");
            return map;
        } else {
            System.out.println("Can't serialize: " + v.getClass());
            System.out.println(v);
            throw new IllegalArgumentException();
        }
    }

    public static String convertTimedNumberTimestamp(TimedNumber v) {
        return String.valueOf(v.getTime());
    }

    boolean checkRequest(String requestDRF) {
        return ((requestDRF.contains(":") || requestDRF.contains("|") || requestDRF.contains("_")) &&
                StandardCharsets.US_ASCII.newEncoder().canEncode(requestDRF));
    }

    void abort(String response, final HttpServerExchange exchange) throws IOException {
        if (!exchange.isResponseChannelAvailable()) {
            return;
        }
        logger.warning("Aborting: " + response);
        //exchange.startBlocking();
        exchange.setStatusCode(500);
        exchange.getResponseSender().send(response);
        exchange.endExchange();
    }
}

class DIODMQTimedDataCallback extends TimedNumberCallback {

    SynchronousQueue<HashMap<String,TimedNumber>> returnQueue;
    HashMap<String,TimedNumber> returnData;
    private final int requestSize;

    int read_timeout;

    DIODMQTimedDataCallback(Set<String> dataRequest) {
        this(dataRequest, 10000);
    }
    DIODMQTimedDataCallback(Set<String> dataRequest, int timeout) {
        returnQueue = new SynchronousQueue<>();
        returnData = new HashMap<>(dataRequest.size());
        requestSize = dataRequest.size();
        read_timeout = timeout;
    }

    @Override
    public void dataChanged( String dataRequest, TimedNumber data ) {
        if (DIODMQ.isPendingStatus(data))
            return;

        try {
            returnData.put(dataRequest, data);
            if (returnData.size() == requestSize) {
                returnQueue.put(returnData);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public HashMap<String,TimedNumber> getData() {
        try {
            return returnQueue.poll(read_timeout, TimeUnit.MILLISECONDS); //.take();
        } catch (InterruptedException ie) {
            System.out.println("DIODMQ.DataCallback.getData wait interrupted: " + ie.getMessage());
            return null;
        }
    }

}