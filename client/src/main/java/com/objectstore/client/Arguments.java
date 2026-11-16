package com.objectstore.client;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A minimal command-line parser: positional arguments plus {@code --option value...}.
 * Single-valued options take exactly one value; multi-valued options take every argument up to
 * the next option, and may be repeated.
 */
final class Arguments {

    private final List<String> positionals = new ArrayList<>();
    private final Map<String, List<String>> options = new LinkedHashMap<>();

    static Arguments parse(List<String> args, Set<String> singleValued, Set<String> multiValued) {
        Arguments parsed = new Arguments();
        int i = 0;
        while (i < args.size()) {
            String arg = args.get(i++);
            if (!arg.startsWith("--")) {
                parsed.positionals.add(arg);
            } else if (singleValued.contains(arg)) {
                if (i >= args.size()) {
                    throw new CliException(ObjectStoreCli.EXIT_USAGE, arg + " needs a value");
                }
                parsed.options.computeIfAbsent(arg, key -> new ArrayList<>()).add(args.get(i++));
            } else if (multiValued.contains(arg)) {
                List<String> values = parsed.options.computeIfAbsent(arg, key -> new ArrayList<>());
                int before = values.size();
                while (i < args.size() && !args.get(i).startsWith("--")) {
                    values.add(args.get(i++));
                }
                if (values.size() == before) {
                    throw new CliException(ObjectStoreCli.EXIT_USAGE, arg + " needs at least one value");
                }
            } else {
                throw new CliException(ObjectStoreCli.EXIT_USAGE, "unknown option " + arg);
            }
        }
        return parsed;
    }

    List<String> positionals() {
        return positionals;
    }

    /** The last value given for a single-valued option, or {@code null}. */
    String option(String name) {
        List<String> values = options.get(name);
        return values == null || values.isEmpty() ? null : values.get(values.size() - 1);
    }

    List<String> options(String name) {
        return options.getOrDefault(name, List.of());
    }

    /** Splits {@code key=value}; the value may itself contain '='. */
    static Map.Entry<String, String> keyValue(String pair) {
        int equals = pair.indexOf('=');
        if (equals <= 0) {
            throw new CliException(ObjectStoreCli.EXIT_USAGE, "expected key=value but got '" + pair + "'");
        }
        return Map.entry(pair.substring(0, equals), pair.substring(equals + 1));
    }
}
