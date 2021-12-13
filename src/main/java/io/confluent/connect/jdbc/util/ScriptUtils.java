package io.confluent.connect.jdbc.util;

/*-
 * #%L
 * Kafka Connect HTTP
 * %%
 * Copyright (C) 2020 - 2021 Cástor Rodríguez
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.Map;

/**
 * @author Alex.Sun
 * @created 2021-12-01 15:04
 */
@Slf4j
public final class ScriptUtils {
    public static final String VAR_NAME = "offset";

    private static final ScriptEngine scriptEngine = new ScriptEngineManager().getEngineByName("JavaScript");
    public static final String VAR_LOG = "log";

    @SneakyThrows
    public static Map<String, Object> evalScript(String script, Map<String, Object> offset) {
        if (StringUtils.isNotEmpty(script)) {
            Bindings bindings = scriptEngine.createBindings();
            bindings.put(VAR_NAME, offset);
            bindings.put(VAR_LOG, log);
            Map<String, Object> vars = (Map<String, Object>) scriptEngine.eval(script + ";" + VAR_NAME, bindings);
            offset.putAll(vars);
        }

        return offset;
    }
}
