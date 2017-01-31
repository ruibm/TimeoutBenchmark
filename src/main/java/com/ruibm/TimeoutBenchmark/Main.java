package com.ruibm.TimeoutBenchmark;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.net.URL;

public class Main {
  @Option(name = "--cpuThreads", usage = "Number of CPU threads.")
  public int cpuThreads = 1;

  @Option(name = "--networkThreads", usage = "Number of Network threads.")
  public int networkThreads = 1;

  @Option(name = "--ioThreads", usage = "Number of IO threads writing files.")
  public int ioThreads = 1;

  @Option(name = "--benchmarkSeconds", usage = "Benchmark duration in seconds.")
  public int benchmarkDurationSeconds = 10;

  @Option(name = "--url", usage = "URL to test agaist.", required = true)
  public URL url;

  @Option(name = "--noShowProgress", usage = "Switches of progress update.")
  public boolean noShowProgress = false;

  public static void main(String[] args) {
    new Main().run(args);
  }

  public void run(String[] args) {
    CmdLineParser parser = new CmdLineParser(this);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      e.printStackTrace();
      parser.printUsage(System.err);
      System.exit(-3);
    }

    Benchmarker benchmarker = new Benchmarker(!noShowProgress);
    benchmarker.run(networkThreads, cpuThreads, ioThreads, benchmarkDurationSeconds, url);

  }
}
