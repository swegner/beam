package org.apache.beam.sdk.transforms.display.prototype1;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;

import java.util.List;

import autovalue.shaded.com.google.common.common.collect.Lists;

/** */
public class ExampleUsage {

  abstract class ExampleBaseDoFn extends DoFn<Integer, Integer> {
    private final int retryLimit;
    private final DisplayData.Token displayDataToken = DisplayData.init(ExampleBaseDoFn.class);

    protected ExampleBaseDoFn(int retryLimit) {
      this.retryLimit = retryLimit;
      displayDataToken.add(DisplayData.item("retryLimit", retryLimit));
    }

    protected Integer postRequest(String url, Integer input) { return null; }

    @Override
    public DisplayData.Reader getDisplayData() {
      return displayDataToken;
    }
  }

  class ExampleDoFn extends ExampleBaseDoFn {
    private final String token;
    private final int size;
    private final DisplayData.Token displayDataToken = DisplayData.init(ExampleDoFn.class);

    public ExampleDoFn(String token, int size) {
      super(123);

      this.token = token;
      this.size = size;

      this.displayDataToken
          .include(super.getDisplayData())
          .add(DisplayData.item("token", token))
          .add(DisplayData.item("size", size));
    }

    @Override
    public DisplayData.Reader getDisplayData() {
      return displayDataToken;
    }

    private String endpointUrl;
    @Override
    public void startBundle(Context c) throws Exception {
      endpointUrl = lookupCurrentEndpoint("serviceDiscovery.example.com");
      displayDataToken.add(DisplayData.item("restEndpoint", endpointUrl));
    }
    private String lookupCurrentEndpoint(String discoveryService) { return null; }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      c.output(postRequest(endpointUrl, c.element()));
    }
  }

  class ExampleSource extends BoundedSource<Integer> {
    // boilerplate..
    @Override public void validate() {}
    @Override public Coder<Integer> getDefaultOutputCoder() { return null; }
    @Override public long getEstimatedSizeBytes(PipelineOptions options) { return 0; }
    @Override public boolean producesSortedKeys(PipelineOptions options) { return false; }
    @Override public BoundedReader<Integer> createReader(PipelineOptions options) { return null; }

    DisplayData.Token displayDataToken = DisplayData.init(ExampleSource.class);

    @Override
    public List<? extends BoundedSource<Integer>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) {
      displayDataToken
          .add(DisplayData.item("tempLocation", "ex://scratch"))
          .add(DisplayData.item("extractJob", "foo-123"));
      // do work..
      displayDataToken.add(DisplayData.item("recordCount", 42));

      return Lists.newArrayList(this);
    }

    @Override
    public DisplayData.Reader getDisplayData() {
      return displayDataToken;
    }
  }
}


