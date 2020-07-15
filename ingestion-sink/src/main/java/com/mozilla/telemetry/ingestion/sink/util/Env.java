package com.mozilla.telemetry.ingestion.sink.util;

import com.google.cloud.pubsublite.Partition;
import com.mozilla.telemetry.ingestion.core.util.Time;
import io.grpc.StatusException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Env {

  private final Set<String> include;
  private final Map<String, String> env;
  private final Set<String> unused;

  /** Constructor. */
  public Env(Set<String> include) {
    this.include = include;
    env = include.stream().filter(key -> System.getenv(key) != null)
        .collect(Collectors.toMap(key -> key, System::getenv));
    unused = new HashSet<>(env.keySet());
  }

  /**
   * Throw an {@link IllegalArgumentException} for any environment variables set but not used.
   */
  public void requireAllVarsUsed() {
    if (!unused.isEmpty()) {
      throw new IllegalArgumentException("Env vars set but not used: " + unused.toString());
    }
  }

  public boolean containsKey(String key) {
    return env.containsKey(key);
  }

  /** Get the value of an optional environment variable. */
  public Optional<String> optString(String key) {
    if (!include.contains(key)) {
      throw new IllegalArgumentException("key missing from include: " + key);
    }
    unused.remove(key);
    return Optional.ofNullable(env.get(key));
  }

  public String getString(String key) {
    return optString(key)
        .orElseThrow(() -> new IllegalArgumentException("Missing required env var: " + key));
  }

  public String getString(String key, String defaultValue) {
    return optString(key).orElse(defaultValue);
  }

  public List<String> getStrings(String key, List<String> defaultValue) {
    return optString(key).filter(s -> !s.isEmpty()).map(s -> Arrays.asList(s.split(",")))
        .orElse(defaultValue);
  }

  public Integer getInt(String key, Integer defaultValue) {
    return optString(key).map(Integer::new).orElse(defaultValue);
  }

  /** Get the value of an environment variable as a list of PubSub Lite partitions. */
  public List<Partition> getPartitions(String key) throws StatusException {
    List<Partition> partitions = new LinkedList<>();
    for (String num : getString(key).split(",")) {
      partitions.add(Partition.of(Long.parseLong(num)));
    }
    return partitions;
  }

  public Long getLong(String key, Long defaultValue) {
    return optString(key).map(Long::new).orElse(defaultValue);
  }

  public Duration getDuration(String key, String defaultValue) {
    return Time.parseJavaDuration(getString(key, defaultValue));
  }

  public Pattern getPattern(String key) {
    return Pattern.compile(getString(key));
  }
}
