package com.ruibm.TimeoutBenchmark;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.*;
import java.net.URL;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Benchmarker {
  private static final long MAX_THREAD_JOIN_MILLIS = 1000;
  private static final long NETWORK_TIMEOUT_MILLIS = 3000;
  private static final byte[] BUFFER;

  private final OkHttpClient client;
  private final PrintStream progressStream;

  private volatile boolean isBenchmarkRunning;

  static {
    BUFFER = new byte[10 * 1024 * 1024 + 1];
    new Random().nextBytes(BUFFER);
  }

  public Benchmarker(boolean showProgress) {
    this.client = new OkHttpClient.Builder()
        .connectTimeout(NETWORK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .readTimeout(NETWORK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .writeTimeout(NETWORK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
        .build();

    if (showProgress) {
      progressStream = System.out;
    } else {
      progressStream = new PrintStream(ByteStreams.nullOutputStream());
    }
  }

  public void run(int networkThreadCount,
                  int cpuThreadCount,
                  int ioThreadCount,
                  int durationSeconds,
                  URL url) {
    Preconditions.checkArgument(cpuThreadCount >= 0);
    Preconditions.checkArgument(networkThreadCount >= 0);
    Preconditions.checkArgument(ioThreadCount >= 0);
    Preconditions.checkArgument(durationSeconds >= 0);
    Preconditions.checkNotNull(url);
    Stopwatch stopwatch = Stopwatch.createStarted();
    progressStream.println("Benchmark starting...");
    isBenchmarkRunning = true;
    final StatsTracker stats = new StatsTracker(
        networkThreadCount, cpuThreadCount, ioThreadCount);
    List<Thread> threads = Lists.newArrayList();
    if (System.console() != null) {
      threads.addAll(createStartedThread(
          "Monitoring-%d",
          1,
          () -> monitoringThreadMain(stats)));
    }
    threads.addAll(createStartedThread(
        "NetworkThread-%d",
        networkThreadCount,
        () -> networkThreadMain(stats, url)));
    threads.addAll(createStartedThread(
        "IoThread-%d",
        ioThreadCount,
        () -> ioThreadMain(stats)));
    threads.addAll(createStartedThread(
        "CpuThread-%d",
        cpuThreadCount,
        () -> cpuThreadMain(stats)));
    try {
      waitForCurrentBenchmarkToComplete(durationSeconds);
    } finally {
      isBenchmarkRunning = false;
      try {
        for (Thread t : threads) {
          t.join(MAX_THREAD_JOIN_MILLIS);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
        System.exit(-2);
      } finally {
        progressStream.println(String.format(
            "Benchmark finished in [%d seconds].",
            stopwatch.elapsed(TimeUnit.SECONDS)));
        progressStream.println("Here are the final results:");
        String msg = String.format("%s (%s)", stats.getBenchmarkSummary(), url);
        System.out.println(msg);
      }
    }
  }

  private static List<Thread> createStartedThread(
      String nameTemplate, int threadCount, Runnable runnable) {
    List<Thread> threads = Lists.newArrayList();
    for (int i = 0; i < threadCount; ++i) {
      Thread thread = new Thread(runnable);
      thread.setName(String.format(nameTemplate, i));
      thread.start();
      threads.add(thread);
    }

    return threads;
  }

  private void waitForCurrentBenchmarkToComplete(int durationSeconds) {
    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(durationSeconds));
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private void monitoringThreadMain(StatsTracker stats) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    String progress = "-\\|/";
    int progressCharIndex = 0;
    while (isBenchmarkRunning) {
      progressCharIndex = (progressCharIndex + 1) % progress.length();
      try {
        String line = String.format(
            "\r[%s] %s Elapsed=[%d secs]",
            progress.charAt(progressCharIndex),
            stats.getStatusLine(),
            stopwatch.elapsed(TimeUnit.SECONDS));
        progressStream.print(line);
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }

    System.out.println();
  }

  private void cpuThreadMain(StatsTracker stats) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (isBenchmarkRunning) {
      // Tight loop non stop.
      stats.logCpuCycleMillis(stopwatch.elapsed(TimeUnit.MICROSECONDS));
      stopwatch.reset().start();
    }
  }

  private void networkThreadMain(StatsTracker stats, URL url) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (isBenchmarkRunning) {
      try {
        Request request = new Request.Builder().get().url(url).build();
        try (ByteArrayOutputStream os = new ByteArrayOutputStream();
             Response response = client.newCall(request).execute()) {
          ByteStreams.copy(response.body().byteStream(), os);
          stats.logSuccessfulNetworkRequest(
              response.receivedResponseAtMillis() - response.sentRequestAtMillis());
        }
      } catch (Exception e) {
        stats.logNetworkError(e);
      }
      stats.logNetworkCycle(stopwatch.elapsed(TimeUnit.MILLISECONDS));
      stopwatch.reset().start();
    }
  }

  private void ioThreadMain(StatsTracker stats) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    File dir = new File("./.tmp");
    if (!dir.exists()) {
      dir.mkdir();
    }
    File file = new File(
        dir,
        "TimeoutBenchmark_" + Thread.currentThread().getName() + ".bin");


    while (isBenchmarkRunning) {
      try {

        try (OutputStream outputStream = new FileOutputStream(file)) {
          outputStream.write(BUFFER);
        }
      } catch (IOException e) {
        stats.logIoException(e);
      }

      stats.logIoCycle(stopwatch.elapsed(TimeUnit.MICROSECONDS));
      stopwatch.reset().start();
    }
  }
}
