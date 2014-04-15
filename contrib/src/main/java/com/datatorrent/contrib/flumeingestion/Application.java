/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.flumeingestion;

import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.annotation.ShipContainingJars;

import com.datatorrent.flume.operator.AbstractFlumeInputOperator;
import com.datatorrent.lib.bucket.TimeBasedBucketManagerImpl;
import com.datatorrent.lib.dedup.DeduperWithHdfsStore;

import static com.datatorrent.lib.dedup.Deduper.CountersListener;

@ApplicationAnnotation(name = "FlumeIngestion")
public class Application implements StreamingApplication
{
  public static final byte FIELD_SEPARATOR = 1;
  public static final String FLUME_SINK_ADDRESSES = "flumeSinkAddresses";
  public static final String INPUT_OPERATOR = "inputOperator";
  public static final String SKIP_DEDUPER = "skipDeduper";

  @ShipContainingJars(classes = {Configurable.class, RetryPolicy.class, ServiceInstance.class, Context.class, CuratorFramework.class, DateTimeFormat.class})
  public static class FlumeInputOperator extends AbstractFlumeInputOperator<FlumeEvent>
  {

    @Override
    public FlumeEvent convert(Event event)
    {
      return FlumeEvent.from(event.getBody(), FIELD_SEPARATOR, 1);
    }
  }

  public static class FlumeEventDeduper extends DeduperWithHdfsStore<FlumeEvent, FlumeEvent>
  {
    @Override
    protected FlumeEvent convert(FlumeEvent flumeEvent)
    {
      return flumeEvent;
    }
  }

  private Context getFlumeContext(Configuration conf, String prefix)
  {
    Context context = new Context();

    context.put("serviceName", "DTFlume");
    context.put("connectionString", "localhost:2181");
    context.put("connectionTimeoutMillis", "1000");
    context.put("connectionRetryCount", "10");
    context.put("connectionRetrySleepMillis", "500");
    context.put("basePath", "/HelloDT");

    for (Entry<String, String> entry : context.getParameters().entrySet()) {
      String value = conf.get(prefix + entry.getKey());
      if (value != null) {
        context.put(entry.getKey(), value);
      }
    }

    return context;
  }

  public static class BlackHole extends BaseOperator
  {
    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
      }

    };
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String inputType = conf.get(INPUT_OPERATOR, "simulate");
    boolean skipDeduper = conf.getBoolean(SKIP_DEDUPER, false);

    String[] dtFlumeAdapterAddresses = conf.getStrings(FLUME_SINK_ADDRESSES, new String[]{"0:localhost:8080"});
    /*
     * set the streaming window to be 1 second long.
     */
    dag.setAttribute(DAGContext.STREAMING_WINDOW_SIZE_MILLIS, 1000);
    dag.setAttribute(PortContext.QUEUE_CAPACITY, 16 * 1024);

    DefaultOutputPort<FlumeEvent> feedPort;
    if (inputType.equals("simulate")) {
      InputSimulator simulator = dag.addOperator("InputSimulator", new InputSimulator());
      simulator.setFilePath(conf.get(getClass().getName() + ".InputSimulator.filePath", "dt_test_data"));
      feedPort = simulator.output;
    }
    else if (inputType.equals("hdfs")) {
      HdfsInputOperator hdfsInput = dag.addOperator("HdfsInput", new HdfsInputOperator());
      feedPort = hdfsInput.output;
    }
    else {
      DummyInputOperator dummyInputOperator = dag.addOperator("Dummy", new DummyInputOperator());
      feedPort = dummyInputOperator.output;
    }
//    else {
//      FlumeInputOperator inputOperator = dag.addOperator("FlumeIngestor", new FlumeInputOperator());
//      inputOperator.setConnectAddresses(dtFlumeAdapterAddresses);
//      inputOperator.setCodec(new EventCodec());
//
//      /* initialize auto discovery mechanism for the operator */
//      FlumeInputOperator.ZKStatsListner statsListener = new FlumeInputOperator.ZKStatsListner();
//      statsListener.configure(getFlumeContext(conf, "FlumeIngestor.DiscoveryContext."));
//      statsListener.setIntervalMillis(60 * 1000);
//      dag.setAttribute(inputOperator, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{statsListener}));
//
//      feedPort = inputOperator.output;
//    }

    BlackHole blackHole = dag.addOperator("BlackHole", new BlackHole());

    /*
     * Dedupe the flume events bucketData.
     */
    if (!skipDeduper) {
      FlumeEventDeduper deduper = dag.addOperator("Deduper", new FlumeEventDeduper());
      TimeBasedBucketManagerImpl<FlumeEvent> bucketManager = new TimeBasedBucketManagerImpl<FlumeEvent>();
      bucketManager.initialize();
      deduper.setBucketManager(bucketManager);

      CountersListener statsListener = new CountersListener();
      dag.setAttribute(deduper, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{statsListener}));
      dag.setAttribute(deduper, OperatorContext.APPLICATION_WINDOW_COUNT, 120);

      dag.addStream("FlumeEvents", feedPort, deduper.input);
      dag.addStream("DedupedEvents", deduper.output, blackHole.input);
    }
    else {
      dag.addStream("FlumeEvents", feedPort, blackHole.input);
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(Application.class);
}
