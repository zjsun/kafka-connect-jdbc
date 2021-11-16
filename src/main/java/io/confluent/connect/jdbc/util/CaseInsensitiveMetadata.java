package io.confluent.connect.jdbc.util;

import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Alex.Sun
 * @created 2021-11-16 10:05
 */
public class CaseInsensitiveMetadata {
    public static <R> R tryCaseInsensitive(String[] args, ResultsetFunction<String[], R> function, Function<R, Boolean> ifBreak, R initRet) throws SQLException {
        List<String[]> list = new ArrayList<>();
        list.add(args);
        list.add(Arrays.stream(args).map(s -> org.apache.commons.lang3.StringUtils.lowerCase(s)).collect(Collectors.toList()).toArray(new String[0]));
        list.add(Arrays.stream(args).map(s -> StringUtils.upperCase(s)).collect(Collectors.toList()).toArray(new String[0]));

        R ret = initRet;
        for (String[] arg : list) {
            ret = function.apply(arg);
            if (ifBreak.apply(ret)) break;
        }
        return ret;
    }

    public interface ResultsetFunction<T, R> {
        R apply(T t) throws SQLException;
    }
}
