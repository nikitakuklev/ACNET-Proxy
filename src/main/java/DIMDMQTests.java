import gov.fnal.controls.daq.acquire.SettingsState;
import gov.fnal.controls.tools.dio.DIODMQ;
import gov.fnal.controls.tools.timed.TimedNumber;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DIMDMQTests {
    static Random rnd = new Random();

    public static void main(String[] args) throws Exception {
        // Enable ahead of time to prevent time loss
        DIODMQ.enableSettings(true, SettingsState.FOREVER);
        Thread.sleep(1000);
        List<String> devices = IntStream.range(0, 10).mapToObj(i -> String.format("G:CHIP[%d]", i)).collect(Collectors.toList());
        devices.add("Z:ACLTST");
        devices.add("Z:ACLTST[1]");
        devices.add("Z:ACLTST[2]");
        System.out.println(devices);
        // Main loop
        int iteration = 0;
        while (true) {
            System.out.println(">>>>Iteration: " + iteration);
            for (int i = 0; i < 10; i++) {
                int idx = rnd.nextInt(devices.size());
                long t0 = System.nanoTime();
                TimedNumber setting = DIODMQ.setDevice(devices.get(idx).replace(":", "_"),
                        4.0 + rnd.nextFloat() * 2);
                long t1 = System.nanoTime();
                TimedNumber readback = DIODMQ.readDevice(devices.get(idx) + "@I");
                long t2 = System.nanoTime();
                long tsleep = 10000 + 800 * rnd.nextInt(10);
                System.out.println(String.format("Loop  %3d (%11s): set %7.2f | read %7.2f | %s | %s",
                        i, devices.get(idx), (t1 - t0) / 1e6, (t2 - t1) / 1e6, setting, readback));
                if ((t1 - t0) / 1e6 > 1000 | (t2 - t1) / 1e6 > 1000) {
                    throw new Exception();
                }
                Thread.sleep(tsleep);
            }
            for (int idx = 0; idx < devices.size(); idx++) {
                long t0 = System.nanoTime();
                TimedNumber setting = DIODMQ.setDevice(devices.get(idx).replace(":", "_"),
                        4.0 + rnd.nextFloat() * 2);
                long t1 = System.nanoTime();
                long t2 = System.nanoTime();
                System.out.println(String.format("Burst %3d (%11s): set %7.2f | read %7.2f | %s | %s",
                        idx, devices.get(idx), (t1 - t0) / 1e6, (t2 - t1) / 1e6, setting, "n/a"));
                if ((t1 - t0) / 1e6 > 1000 | (t2 - t1) / 1e6 > 1000) {
                    throw new Exception();
                }
            }
            iteration++;
        }
    }
}

