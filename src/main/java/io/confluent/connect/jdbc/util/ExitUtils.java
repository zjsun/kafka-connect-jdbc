package io.confluent.connect.jdbc.util;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

/**
 * @author Alex.Sun
 * @created 2021-11-24 10:14
 */
public class ExitUtils {
    public static void forceExit(int code, String msg, int timeoutInSecond){
        new Thread(() -> {
            Utils.sleep(timeoutInSecond * 1000);
            Exit.halt(code, msg);
        }).start();
        Exit.exit(code, msg);
    }
}
