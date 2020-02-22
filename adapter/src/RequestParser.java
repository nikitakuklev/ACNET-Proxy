import gov.fnal.controls.tools.drf2.DiscreteRequest;
import gov.fnal.controls.tools.drf2.RequestFormatException;
import gov.fnal.controls.webapps.getdaq.web.errors.BadRequestException;
import gov.fnal.controls.webapps.getdaq.web.errors.InternalErrorException;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class is used for buiding Request object. Contains methods used for parsing request string.<br>
 * By default, DAQ requests are considered to be separated by <b>semicolon</b>.
 * @author Alexey Moroz
 * @version 1.0
 */
public abstract class RequestParser {

    /**
     * Parses string provided into an array of separate DAQ requests.
     * @param request string to be parsed
     * @return DAQ requests
     * @throws InternalErrorException if internal error occurs
     */
    public static String[] parseRequest(String request) throws InternalErrorException {
        return parseRequest(request, true);
    }

    /**
     * Parses string provided into an array of separate DAQ requests.
     * @param request string to be parsed
     * @param semicolonSeparated semicolon-separated or brackets-separated string flag
     * @return DAQ requests
     * @throws InternalErrorException if internal error occurs
     */
    public static String[] parseRequest(String request, boolean semicolonSeparated) throws InternalErrorException {
        if(request==null){
            return new String[0];
        }
        List<String> returnValue = new ArrayList<String>(32);
        try{
            String token;
            String group;
            if(semicolonSeparated){ // semicolon-separated
                StringTokenizer tokenizer = new StringTokenizer(request,";\r\n \f\t");
                while(tokenizer.hasMoreTokens()){
                    token = tokenizer.nextToken();
                    if(!token.equalsIgnoreCase("")){returnValue.add(token);}
                }
            }else{ // brackets-separated
                Pattern pattern=Pattern.compile(
                        "(\\([a-zA-Z0-9_@\\.\\[\\]:;!#\\$%\\^\\&\\*\\-\\+=<>/\\?,~'\"`\\{\\}\\|\\\\]*\\)){1}",
                        Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(request);
                while(matcher.find()){
                    group = matcher.group().replaceAll("[\\(|\\)]", "").trim();
                    if(!group.equalsIgnoreCase("")){
                        returnValue.add(group);
                    }
                }
            }
        }catch(Exception e){
            throw new InternalErrorException(
                    "RequestParser: Internal error occured during parsing of DAQ request: " + request + "\r\n" +
                            e.getClass().getCanonicalName() + " had been thrown with message: " + e.getMessage()
            );
        }
        return returnValue.toArray(new String[0]);
    }

    /**
     * Returns an array of device entities corresponding to the request string.
     * @param request string with semicolon-separated DAQ requests
     * @return device entities
     * @throws InternalErrorException if internal error occurs
     * @throws BadRequestException if DAQ request is in wrong format
     */
    public static DeviceEntity[] getDeviceEntities(String request) throws InternalErrorException, BadRequestException {
        return getDeviceEntities(parseRequest(request));
    }

    /**
     * Returns an array of device entities corresponding to the request string.
     * @param request string with semicolon-separated DAQ requests
     * @param semicolonSeparated true for semicolon-separated and false for brackets-separated request
     * @return device entities
     * @throws InternalErrorException if internal error occurs
     * @throws BadRequestException if DAQ request is in wrong format
     */
    public static DeviceEntity[] getDeviceEntities(String request, boolean semicolonSeparated) throws InternalErrorException, BadRequestException {
        return getDeviceEntities(parseRequest(request, semicolonSeparated));
    }

    /**
     * Returns an array of device entities corresponding to the queries provided.
     * @param daqQueries DAQ queries
     * @return device entities
     * @throws BadRequestException if DAQ request is in wrong format
     */
    @SuppressWarnings("static-access")
    public static DeviceEntity[] getDeviceEntities(String[] daqQueries) throws BadRequestException{
        if(daqQueries.length<1){
            return new DeviceEntity[0];
        }
        List<DeviceEntity> requests = new ArrayList<DeviceEntity>(32);
        try{
            for(String parsedQuery: daqQueries){
                requests.add(new DeviceEntity((DiscreteRequest)DiscreteRequest.parse(parsedQuery)));
            }
        }catch(RequestFormatException rfe){
            throw new BadRequestException( "RequestParser: " +
                    rfe.getClass().getCanonicalName() + " had been thrown with message: " + rfe.getMessage()
            );
        }
        return requests.toArray(new DeviceEntity[0]);
    }

}