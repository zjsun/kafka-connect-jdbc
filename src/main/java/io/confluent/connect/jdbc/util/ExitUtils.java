package io.confluent.connect.jdbc.util;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Alex.Sun
 * @created 2021-11-24 10:14
 */
public class ExitUtils {

    public static final String MSG_DONE = "[此异常为正常退出，请忽略]";

    private static final AtomicBoolean exiting = new AtomicBoolean(false);

    public static void forceExit(int code, String msg) {
        if (!exiting.get()) {
            exiting.set(true);

            new Thread(() -> {
                Utils.sleep(60000);
                Exit.halt(code, msg);
            }).start();

            Exit.exit(code, msg);
        }
    }
}
