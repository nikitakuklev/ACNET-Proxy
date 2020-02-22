import gov.fnal.controls.daq.drf2.DataCallback;
import gov.fnal.controls.service.proto.DAQData;
import gov.fnal.controls.service.proto.DAQData.Reply;
import gov.fnal.controls.tools.daqdata.DAQDataXMLTransform;
import gov.fnal.controls.tools.drf2.DiscreteRequest;

/**
 * Class holds references to DRF2 request, reply and callback incorporated in one object.
 * @author Alexey Moroz
 * @version 1.0
 */
@SuppressWarnings("unqualified-field-access")
public final class DeviceEntity{

    /**
     * Device requested
     */
    private final DiscreteRequest device;

    /**
     * Reply
     */
    private volatile DAQData.Reply reply;

    /**
     * Callback
     */
    private final DataCallback callback;

    /**
     * Default constructor.
     * @param device device requested
     */
    public DeviceEntity(DiscreteRequest device){
        this.device = device;
        this.callback = new DeviceCallback();
        this.reply = DAQDataXMLTransform.createPendingStatus();
    }

    /**
     * Extended constructor, you must provide your own callback.
     * @param device device requested
     * @param callback data callback
     */
    public DeviceEntity(DiscreteRequest device, DataCallback callback){
        this.device = device;
        this.callback = callback;
        this.reply = null;
    }

    /**
     * Returns DiscreteRequest.equals(Object obj).<br>
     * DeviceEntity and DiscreteRequest are the 'same' objects.
     */
    @Override
    public boolean equals(Object obj) {
        if(obj instanceof DeviceEntity){
            return this.device.equals(((DeviceEntity)obj).device);
        }else{
            return false;
        }
    }

    /**
     * Returns DiscreteRequest.hashCode().<br>
     * DeviceEntity and DiscreteRequest are the 'same' objects.
     */
    @Override
    public int hashCode() {
        return device.hashCode();
    }

    /**
     * Returns DiscreteRequest.toString().<br>
     * DeviceEntity and DiscreteRequest are the 'same' objects.
     */
    @Override
    public String toString() {
        return device.toString();
    }

    /**
     * Returns device requested.
     * @return device
     */
    public DiscreteRequest getDevice() {
        return device;
    }

    /**
     * Returns callback.
     * @return callback
     */
    public DataCallback getCallback() {
        return callback;
    }

    /**
     * Returns reply.
     * @return reply
     */
    public DAQData.Reply getReply() {
        return reply; // volatile
    }

    /**
     * Device's data callback.
     * @author Alexey Moroz
     * @version 1.0
     */
    protected class DeviceCallback implements DataCallback {

        @SuppressWarnings("synthetic-access")
        @Override
        public void dataChanged(Reply daqReply) {
            synchronized(reply){
                reply=daqReply;
            }
        }

    }

}