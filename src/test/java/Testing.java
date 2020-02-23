import gov.fnal.controls.acnet.AcnetError;
import gov.fnal.controls.service.dmq.DaqClient;
import gov.fnal.controls.service.dmq.DefaultCredentialHandler;
import gov.fnal.controls.service.proto.DAQData;
import gov.fnal.controls.tools.timed.TimedError;
import gov.fnal.controls.tools.timed.TimedNumberFactory;
import gov.fnal.controls.webapps.getdaq.web.errors.BadRequestException;
import gov.fnal.controls.webapps.getdaq.web.errors.InternalErrorException;
import gov.fnal.controls.webapps.getdaq.web.errors.ServiceUnavailableException;
import gov.fnal.controls.webapps.getdaq.web.ContentType;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

public class Testing {
//    public static void main(String[] args) throws BadRequestException, InternalErrorException, ServiceUnavailableException, InterruptedException {
////        System.out.println("HI");
////        Settings settings = Settings.getInstance();
////        CacheManager cacheManager = CacheManager.getInstance(settings);
////        Request requestEntity;
////        requestEntity = new Request("Z:ACLTST;M:OUTTMP", true);
////        if(requestEntity.isValidRequest()){
////            cacheManager.submitRequest(requestEntity);
////        }else{
////            throw new BadRequestException("Request string is not valid");
////        }
////        boolean quiet=false;
////        boolean isoTime=false;
////        boolean includeDrf2IntoHtml=false;
////        for (int i=0;i<10;i++) {
////            String reply = Marshaller.marshall(requestEntity, cacheManager.getReplies(requestEntity), ContentType.PLAIN, isoTime, quiet, includeDrf2IntoHtml);
////            System.out.println(reply);
////            TimeUnit.SECONDS.sleep(1);
////        }
////
////    }

    public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
//        DaqClient daqClient = new DaqClient();
//        daqClient.setCredentialHandler(new DefaultCredentialHandler());
//        String[] var2 = new String[]{"Z_ACLTST","M_OUTTMP"};
//
//        java.lang.reflect.Constructor<?> bConstructor = Class.forName("gov.fnal.controls.tools.dio.DIODMQSettingJob").getDeclaredConstructor();
//        bConstructor.setAccessible(true);
//        Object settingJob = bConstructor.newInstance(daqClient, var2);
//
//        TimedError var4 = new TimedError(0, 0);
//
//        Object var6 = settingJob;//DIODMQSettingJob var6 = new DIODMQSettingJob(daqClient, var2);
//        //DIODMQSettingCallback var3 = (DIODMQSettingCallback)var6.getDataCallback();
//        //var3.getData();
//        var6.getDataCallback().getData();
//
//        //DIODMQSettingCallback var7 = (DIODMQSettingCallback)var6.getDataCallback();
//        var6.getDataCallback().setData(var0, var1);
//
//        Object var5 = var6.getDataCallback().getData();
//        //DAQData.Reply var3 = TimedNumberFactory.toProto(var2)

    }
}
