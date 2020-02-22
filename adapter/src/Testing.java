import gov.fnal.controls.webapps.getdaq.web.errors.BadRequestException;
import gov.fnal.controls.webapps.getdaq.web.errors.InternalErrorException;
import gov.fnal.controls.webapps.getdaq.web.errors.ServiceUnavailableException;
import gov.fnal.controls.webapps.getdaq.web.ContentType;

import java.util.concurrent.TimeUnit;

public class Testing {
    public static void main(String[] args) throws BadRequestException, InternalErrorException, ServiceUnavailableException, InterruptedException {
        System.out.println("HI");
        Settings settings = Settings.getInstance();
        CacheManager cacheManager = CacheManager.getInstance(settings);
        Request requestEntity;
        requestEntity = new Request("Z:ACLTST;M:OUTTMP", true);
        if(requestEntity.isValidRequest()){
            cacheManager.submitRequest(requestEntity);
        }else{
            throw new BadRequestException("Request string is not valid");
        }
        boolean quiet=false;
        boolean isoTime=false;
        boolean includeDrf2IntoHtml=false;
        for (int i=0;i<10;i++) {
            String reply = Marshaller.marshall(requestEntity, cacheManager.getReplies(requestEntity), ContentType.PLAIN, isoTime, quiet, includeDrf2IntoHtml);
            System.out.println(reply);
            TimeUnit.SECONDS.sleep(1);
        }

    }

}
