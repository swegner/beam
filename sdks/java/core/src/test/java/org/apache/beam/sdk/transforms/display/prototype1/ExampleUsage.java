package org.apache.beam.sdk.transforms.display.prototype1;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;

import com.google.common.collect.Lists;

import java.util.List;

/** */
public class ExampleUsage {

  class ExampleDoFn extends DoFn<Integer, Integer> {
    private final String token;
    private final int size;
    public ExampleDoFn(String token, int size) {
      this.token = token;
      this.size = size;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      // No changes from current API
      super.populateDisplayData(builder);
      builder
          .add(DisplayData.item("token", token))
          .add(DisplayData.item("size", size));

      // Dynamic display data
      builder.addIfNotNull(DisplayData.item("restEndpoint", endpointUrl));
    }

    private String endpointUrl;
    @Override
    public void startBundle(Context c) throws Exception {
      endpointUrl = lookupCurrentEndpoint("serviceDiscovery.example.com");
    }
    private String lookupCurrentEndpoint(String discoveryService) { return null; }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      c.output(postRequest(endpointUrl, c.element()));
    }
    private Integer postRequest(String url, Integer input) { return null; }
  }



  class ExampleSource extends BoundedSource<Integer> {
    // boilerplate..
    @Override public void validate() {}
    @Override public Coder<Integer> getDefaultOutputCoder() { return null; }
    @Override public long getEstimatedSizeBytes(PipelineOptions options) { return 0; }
    @Override public boolean producesSortedKeys(PipelineOptions options) { return false; }
    @Override public BoundedReader<Integer> createReader(PipelineOptions options) { return null; }

    // Initialized during runtime
    private String tempLocation;
    private String extractJob;
    private int recordCount;

    @Override
    public List<? extends BoundedSource<Integer>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) {
      tempLocation = "ex://scratch1234";
      extractJob = "foo-123";
      // do work..
      recordCount = 42;

      return Lists.newArrayList(this);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      // Dynamic display data
      builder
          .addIfNotNull(DisplayData.item("tempLocation", tempLocation))
          .addIfNotNull(DisplayData.item("extractJob", extractJob))
          .addIfNotNull(DisplayData.item("recordCount", recordCount));

    }
  }
}


