package com.ruibm.TimeoutBenchmark;

import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;

public class StatsTracker {
  private final LongBinaryOperator SUM = (a, b) -> a + b;

  private final int networkThreadCount;
  private final int cpuThreadCount;
  private final int ioThreadCount;

  private final AtomicLong networkCallCount;
  private final AtomicLong networkCycleMillis;
  private final AtomicLong networkLatencyMillis;
  private final AtomicLong networkLatencyMillisCount;
  private final AtomicLong networkErrorCount;
  private final AtomicLong cpuCallCount;
  private final AtomicLong cpuLatencyMicroseconds;
  private final AtomicLong ioCallCount;
  private final AtomicLong ioExceptionCount;
  private final AtomicLong ioLatencyMicroseconds;
  private final Map<String, AtomicLong> exceptions;

  public StatsTracker(int networkThreads, int cpuThreads, int ioThreads) {
    this.networkThreadCount = networkThreads;
    this.cpuThreadCount = cpuThreads;
    this.ioThreadCount = ioThreads;
    this.networkCallCount = new AtomicLong();
    this.networkCycleMillis = new AtomicLong();
    this.networkLatencyMillisCount = new AtomicLong();
    this.networkLatencyMillis = new AtomicLong();
    this.networkErrorCount = new AtomicLong();
    this.cpuCallCount = new AtomicLong();
    this.cpuLatencyMicroseconds = new AtomicLong();
    this.ioCallCount = new AtomicLong();
    this.ioLatencyMicroseconds = new AtomicLong();
    this.ioExceptionCount = new AtomicLong();
    this.exceptions = Maps.newConcurrentMap();
  }

  private static double safeDivision(long numerator, long denominator) {
    if (denominator == 0) {
      return 0;
    }

    return numerator / (double) denominator;
  }

  public void logCpuCycleMillis(long microseconds) {
    cpuCallCount.incrementAndGet();
    cpuLatencyMicroseconds.accumulateAndGet(microseconds, SUM);
  }

  public void logSuccessfulNetworkRequest(long networkMillis) {
    networkLatencyMillis.accumulateAndGet(networkMillis, SUM);
    networkLatencyMillisCount.incrementAndGet();
  }

  public void logNetworkCycle(long cycleMillis) {
    networkCallCount.incrementAndGet();
    networkCycleMillis.accumulateAndGet(cycleMillis, SUM);
  }

  public void logNetworkError(Exception e) {
    networkErrorCount.incrementAndGet();
    String name = e.getClass().getName();
    synchronized (exceptions) {
      if (!exceptions.containsKey(name)) {
        exceptions.put(name, new AtomicLong(0));
      }
    }

    exceptions.get(name).incrementAndGet();
  }

  public void logIoCycle(long microseconds) {
    ioCallCount.incrementAndGet();
    ioLatencyMicroseconds.accumulateAndGet(microseconds, SUM);

  }

  public void logIoException(IOException e) {
  }

  public String getStatusLine() {
    List<String> statusLines = Lists.newArrayList();
    if (cpuThreadCount > 0) {
      statusLines.add(String.format(
          "Cpu=[threads:%d cycles:%d]",
          cpuThreadCount,
          cpuCallCount.get()));
    }

    if (ioThreadCount > 0) {
      statusLines.add(String.format(
          "IO=[threads:%d cycles:%d errors=%d]",
          ioThreadCount,
          ioCallCount.get(),
          ioExceptionCount.get()));
    }

    if (networkThreadCount > 0) {
      statusLines.add(String.format(
          "Network=[threads:%d cycles=%d errors=%d (%.2f%%) latency=%.2fms]",
          networkThreadCount,
          networkCallCount.get(),
          networkErrorCount.get(),
          100 * safeDivision(networkErrorCount.get(), networkCallCount.get()),
          safeDivision(networkLatencyMillis.get(), networkLatencyMillisCount.get())));
    }

    return Joiner.on(" ").join(statusLines);
  }

  public String getExceptionSummary() {
    if (exceptions.isEmpty()) {
      return "";
    }

    List<String> orderedExceptionNames = Lists.newArrayList(exceptions.keySet());
    orderedExceptionNames.sort((a, b) -> Long.compare(
        exceptions.get(b).get(), exceptions.get(a).get()));
    StringBuilder builder = new StringBuilder();
    builder.append("Exceptions: [");
    ImmutableList<String> pairs = FluentIterable
        .from(orderedExceptionNames)
        .transform(name -> String.format(
            "{%s: %d}",
            name,
            exceptions.get(name).get())).toList();
    builder.append(Joiner.on(' ').join(pairs));
    builder.append("]");
    return builder.toString();
  }

  public String getBenchmarkSummary() {
    StringBuilder builder = new StringBuilder();
    builder.append(getStatusLine());
    String exceptionSummary = getExceptionSummary();
    if (!exceptionSummary.isEmpty()) {
      builder.append(" " + exceptionSummary);
    }

    return builder.toString();
  }

  @Override
  public String toString() {
    return getBenchmarkSummary();
  }
}
