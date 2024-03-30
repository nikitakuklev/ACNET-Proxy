import java.util.HashMap;

public class Message {
    public String request;
    public String requestValue = "";
    public String requestType;

    public HashMap<String,String> requestOptions;

    public String response = "";
    public HashMap<String,HashMap<String,String>> responseJson;

    public HashMap<String,String> responseTimestampJson;

    public String responseTime = "";
}
