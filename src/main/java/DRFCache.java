import java.util.concurrent.ConcurrentHashMap;
import gov.fnal.controls.tools.timed.TimedNumber;

public class DRFCache {
    public static ConcurrentHashMap<String, TimedNumber> CACHE = null;
    public static ConcurrentHashMap<String, String> NAMEMAP = null;
}