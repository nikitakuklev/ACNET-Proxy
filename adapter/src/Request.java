import gov.fnal.controls.webapps.getdaq.web.errors.BadRequestException;
import gov.fnal.controls.webapps.getdaq.web.errors.InternalErrorException;

/**
 * Represents a request from client.<br>
 * By default, DAQ requests are considered to be separated by <b>semicolon</b>.
 * @author Alexey Moroz
 * @version 1.0
 */
@SuppressWarnings("unqualified-field-access")
public final class Request {

    /**
     * Unparsed request string
     */
    private final String unparsedRequest;

    /**
     * Parsed DAQ requests
     */
    private final String[] requests;

    /**
     * Devices requested
     */
    private final DeviceEntity[] requestedDevices;

    /**
     * Validity of request
     */
    private boolean invalidRequest=true;

    /**
     * Builds request object
     * @param unparsedRequest unparsed request string
     * @throws BadRequestException if request is in wrong format
     * @throws InternalErrorException  if internal error occurred
     */
    public Request(String unparsedRequest) throws InternalErrorException, BadRequestException{
        this(unparsedRequest, true);
    }

    /**
     * Builds request object
     * @param unparsedRequest unparsed request string
     * @param semicolonSeparated must be true if request is semicolon-separated, false otherwise
     * @throws InternalErrorException if internal error occurred
     * @throws BadRequestException if request is in wrong format
     */
    public Request(String unparsedRequest, boolean semicolonSeparated) throws InternalErrorException, BadRequestException{
        if(unparsedRequest==null){
            this.unparsedRequest="";
        }else{
            this.unparsedRequest=unparsedRequest;
        }
        requests=RequestParser.parseRequest(unparsedRequest, semicolonSeparated);
        requestedDevices=RequestParser.getDeviceEntities(requests);
        invalidRequest=false;
    }

    /**
     * Returns TRUE if this request is invalid.
     * @return true if invalid
     */
    public boolean isInvalidRequest() {
        return invalidRequest;
    }

    /**
     * Returns TRUE when this request is valid.
     * @return true if valid
     */
    public boolean isValidRequest(){
        return !invalidRequest;
    }

    /**
     * Returns unparsed request string.
     * @return unparsed request
     */
    public String getUnparsedRequest() {
        return unparsedRequest;
    }

    /**
     * Returns DAQ requests.
     * @return DAQ requests
     */
    public String[] getRequests() {
        return requests;
    }

    /**
     * Returns device entities representing this request.
     * @return device entities
     */
    public DeviceEntity[] getRequestedDevices() {
        return requestedDevices;
    }

}