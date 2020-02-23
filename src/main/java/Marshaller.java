import org.w3c.dom.Element;

import gov.fnal.controls.service.proto.DAQData.Reply;
import gov.fnal.controls.tools.daqdata.DAQDataHTMLTransform;
import gov.fnal.controls.tools.daqdata.DAQDataJSONTransform;
import gov.fnal.controls.tools.daqdata.DAQDataPlainTextTransform;
import gov.fnal.controls.tools.daqdata.DAQDataXMLTransform;
import gov.fnal.controls.webapps.getdaq.web.ContentType;
import gov.fnal.controls.webapps.getdaq.web.errors.InternalErrorException;

/**
 * Class for generating response string.
 *
 * @author Alexey Moroz
 * @version 1.0
 */
public class Marshaller {

    /**
     * Returns MIME type.
     *
     * @param contentType
     *            enum constant
     * @return MIME type
     */
    public static String getContentTypeString(ContentType contentType) {
        switch (contentType) {
            case XML:
                return "application/xml; charset=UTF-8";
            case HTML:
                return "text/html; charset=UTF-8";
            case PLAIN:
                return "text/plain; charset=UTF-8";
            case JSON:
                return "application/json; charset=UTF-8";
            default:
                return "text/plain; charset=UTF-8";
        }
    }

    /**
     * Serializes reply using marshaller and generates reply string.
     *
     * @param request
     *            request
     * @param replies
     *            Reply[] array from CacheManager
     * @param contentType
     *            content-type of the reply
     * @param isoTime
     *            iso-time option
     * @param quiet
     *            <i>quiet</i> option
     * @return java.lang.String containing reply
     * @throws InternalErrorException
     *             if internal error occurs
     */
    @SuppressWarnings("static-access")
    public static String marshall(Request request, Reply[] replies,
                                  ContentType contentType, boolean isoTime, boolean quiet,
                                  boolean includeDrf2IntoHtml) throws InternalErrorException {
        try {
            DAQDataXMLTransform marshaller;
            switch (contentType) {
                case XML:
                    marshaller = new DAQDataXMLTransform(
                            DAQDataXMLTransform.createDocument(),
                            DAQDataXMLTransform.createOptions(isoTime, quiet, true));
                    break;
                case HTML:
                    marshaller = new DAQDataHTMLTransform(
                            DAQDataHTMLTransform.createDocument(),
                            DAQDataHTMLTransform.createOptions(isoTime, quiet,
                                    true, includeDrf2IntoHtml));
                    break;
                case PLAIN:
                    marshaller = new DAQDataPlainTextTransform(
                            DAQDataPlainTextTransform.createDocument(),
                            DAQDataPlainTextTransform.createOptions(isoTime, quiet,
                                    true));
                    break;
                case JSON:
                    marshaller = new DAQDataJSONTransform(
                            DAQDataJSONTransform.createDocument(),
                            DAQDataJSONTransform
                                    .createOptions(isoTime, quiet, true));
                    break;
                default:
                    marshaller = new DAQDataXMLTransform(
                            DAQDataXMLTransform.createDocument(),
                            DAQDataXMLTransform.createOptions(isoTime, quiet, true));
                    break;
            }
            Element root = marshaller.createDataset();
            String[] unparsedRequests = request.getRequests(); 		// order is saved due to
            DeviceEntity[] devices = request.getRequestedDevices(); // List<G> classes used
            if (unparsedRequests.length != devices.length
                    || devices.length != replies.length) {
                throw new InternalErrorException(
                        Marshaller.class.getCanonicalName()
                                + ".marshall(): array sizes are not equal.");
            }
            for (int i = 0; i < devices.length; i++) {
                if (replies[i] != null) {
                    root.appendChild(marshaller.transform(
                            devices[i].getDevice(), replies[i],
                            unparsedRequests[i]));
                }
            }
            marshaller.addRootElement(root);
            return marshaller.toString();
        } catch (Exception e) {
            throw new InternalErrorException(e.getMessage());
        }
    }

}