package io.confluent.connect.jdbc.util;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.Version;
import lombok.SneakyThrows;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * @author Alex.Sun
 * @created 2021-12-13 14:41
 */
public final class FmUtils {
    private static final Configuration configuration = new Configuration(new Version(2, 3, 30)) {{
        setNumberFormat("computer");
    }};

    @SneakyThrows
    public static String eval(String template, Map<String, Object> params) {
        Template tpl = new Template(UUID.randomUUID().toString(), new StringReader(template), configuration);
        Writer writer = new StringWriter();
        tpl.process(Collections.singletonMap("offset", params), writer);
        return writer.toString();
    }

}
