package org.apache.beam.examples.tutorial.game;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.beam.examples.tutorial.game.utils.Util;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.io.kinesis.KinesisRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableComparator;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

public class TopReferrer {
  public static void main(String[] args) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                                                    .as(PipelineOptions.class);

    Pipeline p = Pipeline.create(options);

    // kinesis stream as source
    p.apply(KinesisIO.Read.from(Util.KINESIS_STREAM, InitialPositionInStream.LATEST)
                          .using(Util.AWS_KEY, Util.AWS_SECRET, Regions.US_EAST_1))
     .apply(Window.into(
       SlidingWindows.of(Duration.standardMinutes(30)).every(Duration.standardSeconds(5))))

     // mapper: extract referrers
     .apply("ExtractReferrers", ParDo.of(new DoFn<KinesisRecord, String>() {
       @ProcessElement
       public void processElement(ProcessContext c) throws Exception {
         String referrer = new String(c.element().getData().array(), StandardCharsets.UTF_8);
         c.output(Util.getReferrer(referrer));
       }
     }))

     // reduce: count for each referrer site
     .apply(Count.<String>perElement())

     // compute top 3 referrers
     .apply(Top.of(3, new SerializableComparator<KV<String, Long>>() {
       @Override
       public int compare(KV<String, Long> o1, KV<String, Long> o2) {
         return Long.compare(o1.getValue(), o2.getValue());
       }
     }).withoutDefaults())

     // display results
     .apply(ParDo.of(new DoFn<List<KV<String, Long>>, String>() {
       @ProcessElement
       public void processElement(ProcessContext c) {
         System.out.print("\n\n");
         for (KV<String, Long> item : c.element()) {
           System.out.println(item.getKey() + ": " + item.getValue());
         }
         System.out.print("\n\n");
       }
     }));

    p.run().waitUntilFinish();
  }

}
