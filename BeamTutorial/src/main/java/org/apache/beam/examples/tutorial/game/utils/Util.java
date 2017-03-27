package org.apache.beam.examples.tutorial.game.utils;

public class Util {
  public static final String KINESIS_STREAM = "KinesisDataVisSampleApp-KinesisStream-SKOMPB95ZCA7";
  public static final String AWS_KEY = "AKIAIU5VN3J3QFCPIVAA";
  public static final String AWS_SECRET = "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";

  private static final String AMZN = "www.amazon.com";
  private static final String GOOG = "www.google.com";
  private static final String RDT = "www.reddit.com";
  private static final String STK = "www.stackoverflow.com";
  private static final String YHOO = "www.yahoo.com";
  private static final String BING = "www.bing.com";

  public static String getReferrer(String ref) {
    if (ref.contains(GOOG))
      return GOOG;
    if (ref.contains(AMZN))
      return AMZN;
    if (ref.contains(YHOO))
      return YHOO;
    if (ref.contains(BING))
      return BING;
    if (ref.contains(STK))
      return STK;
    return RDT;
  }

  // windowing
    // .apply(Window.into(FixedWindows.of(Duration.standardMinutes(3))))


}
