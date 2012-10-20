/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.esotericsoftware.minlog.Log;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.malhartech.lib.testbench.EventClassifier} for three configuration><p>
 * <br>
 * Configuration 1: Provide values and weights<br>
 * Configuration 2: Provide values but no weights (even weights)<br>
 * Configuration 3: Provide no values or weights<br>
 * <br>
 * Benchmarks: Currently does about 3 Million tuples/sec in debugging environment. Need to test on larger nodes<br>
 * <br>
 * Validates all DRC checks of the node<br>
 */
public class EventClassifierTest {

    private static Logger LOG = LoggerFactory.getLogger(EventClassifier.class);

    class TestSink implements Sink {

        HashMap<String, Integer> collectedTuples = new HashMap<String, Integer>();
        HashMap<String, Double> collectedTupleValues = new HashMap<String, Double>();

        int count = 0;
        boolean dohash = true;

        /**
         *
         * @param payload
         */
        @Override
        public void process(Object payload) {
            if (payload instanceof Tuple) {
                // LOG.debug(payload.toString());
            } else {
              count++;
              if (dohash) {
                HashMap<String, Double> tuple = (HashMap<String, Double>)payload;
                for (Map.Entry<String, Double> e: tuple.entrySet()) {
                  Integer ival = collectedTuples.get(e.getKey());
                  if (ival == null) {
                    ival = new Integer(1);
                  }
                  else {
                    ival = ival + 1;
                  }
                  collectedTuples.put(e.getKey(), ival);
                  collectedTupleValues.put(e.getKey(), e.getValue());
                }
              }
            }
        }
        /**
         *
         */
        public void clear() {
            collectedTuples.clear();
            collectedTupleValues.clear();
            count = 0;
        }
    }

    /**
     * Test configuration and parameter validation of the node
     */
    @Test
    public void testNodeValidation() {

        OperatorConfiguration conf = new OperatorConfiguration("mynode", new HashMap<String, String>());
        LoadClassifier node = new LoadClassifier();
        // String[] kstr = config.getTrimmedStrings(KEY_KEYS);
        // String[] vstr = config.getTrimmedStrings(KEY_VALUES);


        conf.set(EventClassifier.KEY_KEYS, "");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + EventClassifier.KEY_KEYS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + EventClassifier.KEY_KEYS,
                    e.getMessage().contains("is empty"));
        }

        conf.set(EventClassifier.KEY_KEYS, "a,b,c"); // from now on keys are a,b,c
        conf.set(EventClassifier.KEY_VALUEOPERATION, "blah");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + EventClassifier.KEY_VALUEOPERATION);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + EventClassifier.KEY_VALUEOPERATION,
                    e.getMessage().contains("not supported. Supported values are"));
        }
        conf.set(EventClassifier.KEY_VALUEOPERATION, "replace"); // from now on valueoperation is "replace"

        conf.set(EventClassifier.KEY_VALUES, "1,2,3,4");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + EventClassifier.KEY_VALUES);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + EventClassifier.KEY_VALUES,
                    e.getMessage().contains("does not match number of keys"));
        }

        conf.set(EventClassifier.KEY_VALUES, "1,2a,3");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + EventClassifier.KEY_VALUES);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + EventClassifier.KEY_VALUES,
                    e.getMessage().contains("Value string should be float"));
        }

        conf.set(EventClassifier.KEY_VALUES, "1,2,3");

        conf.set(EventClassifier.KEY_WEIGHTS, "ia:60,10,35;;ic:20,10,70;id:50,15,35");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + EventClassifier.KEY_WEIGHTS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + EventClassifier.KEY_WEIGHTS,
                    e.getMessage().contains("One of the keys in"));
        }

        conf.set(EventClassifier.KEY_WEIGHTS, "ia:60,10,35;ib,10,75,15;ic:20,10,70;id:50,15,35");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + EventClassifier.KEY_WEIGHTS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + EventClassifier.KEY_WEIGHTS,
                    e.getMessage().contains("need two strings separated by"));
        }

        conf.set(EventClassifier.KEY_WEIGHTS, "ia:60,10,35;ib:10,75;ic:20,10,70;id:50,15,35");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + EventClassifier.KEY_WEIGHTS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + EventClassifier.KEY_WEIGHTS,
                    e.getMessage().contains("does not match the number of keys"));
        }

        conf.set(EventClassifier.KEY_WEIGHTS, "ia:60,10,35;ib:10,75,1a5;ic:20,10,70;id:50,15,35");
        try {
            node.myValidation(conf);
            Assert.fail("validation error  " + EventClassifier.KEY_WEIGHTS);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("validate " + EventClassifier.KEY_WEIGHTS,
                    e.getMessage().contains("Weight string should be an integer"));
        }
    }

    /**
     * Test node logic emits correct results
     */
    @Test
    public void testNodeProcessing() throws Exception {

        EventClassifier node = new EventClassifier();

        TestSink classifySink = new TestSink();
        classifySink.dohash = true;
        node.connect(LoadClassifier.OPORT_OUT_DATA, classifySink);
        OperatorConfiguration conf = new OperatorConfiguration("mynode", new HashMap<String, String>());

        conf.set(EventClassifier.KEY_KEYS, "a,b,c");
        conf.set(EventClassifier.KEY_VALUES, "1,4,5");
        conf.set(EventClassifier.KEY_WEIGHTS, "ia:60,10,35;ib:10,75,15;ic:20,10,70;id:50,15,35");
        conf.set(EventClassifier.KEY_VALUEOPERATION, "replace");

        conf.setInt("SpinMillis", 10);
        conf.setInt("BufferCapacity", 1024 * 1024);
        try {
          node.setup(conf);
        } catch (IllegalArgumentException e) {;}

        HashMap<String, Double> input = new HashMap<String, Double>();
        int sentval = 0;
        for (int i = 0; i < 1000000; i++) {
            input.clear();
            input.put("ia", 2.0);
            input.put("ib", 20.0);
            input.put("ic", 1000.0);
            input.put("id", 1000.0);
            sentval += 4;
            node.process(input);
        }
        node.endWindow();
        int ival = 0;
      if (classifySink.dohash) {
        for (Map.Entry<String, Integer> e: classifySink.collectedTuples.entrySet()) {
          ival += e.getValue().intValue();
        }
      }
      else {
        ival = classifySink.count;
      }

        LOG.info(String.format("\nThe number of keys in %d tuples are %d and %d",
                ival,
                classifySink.collectedTuples.size(),
                classifySink.collectedTupleValues.size()));
        for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
            Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
            Log.info(String.format("%d tuples of key \"%s\" has value %f", ieval.intValue(), ve.getKey(), ve.getValue()));
        }
        Assert.assertEquals("number emitted tuples", sentval, ival);
        // Now test a node with no weights
        EventClassifier nwnode = new EventClassifier();
        classifySink.clear();
        nwnode.connect(EventClassifier.OPORT_OUT_DATA, classifySink);
        conf.set(EventClassifier.KEY_WEIGHTS, "");
        nwnode.setup(conf);

        sentval = 0;
        for (int i = 0; i < 1000000; i++) {
            input.clear();
            input.put("ia", 2.0);
            input.put("ib", 20.0);
            input.put("ic", 1000.0);
            input.put("id", 1000.0);
            sentval += 4;
            nwnode.process(input);
        }
        nwnode.endWindow();
        ival = 0;
      if (classifySink.dohash) {
        for (Map.Entry<String, Integer> e: classifySink.collectedTuples.entrySet()) {
          ival += e.getValue().intValue();
        }
      }
      else {
        ival = classifySink.count;
      }
        LOG.info(String.format("\nThe number of keys in %d tuples are %d and %d",
                ival,
                classifySink.collectedTuples.size(),
                classifySink.collectedTupleValues.size()));
        for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
            Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
            Log.info(String.format("%d tuples of key \"%s\" has value %f", ieval.intValue(), ve.getKey(), ve.getValue()));
        }
        Assert.assertEquals("number emitted tuples", sentval, ival);


        // Now test a node with no weights and no values
        EventClassifier nvnode = new EventClassifier();
        classifySink.clear();
        nvnode.connect(EventClassifier.OPORT_OUT_DATA, classifySink);
        conf.set(EventClassifier.KEY_WEIGHTS, "");
        conf.set(EventClassifier.KEY_VALUES, "");
        nvnode.setup(conf);

        sentval = 0;
        for (int i = 0; i < 1000000; i++) {
            input.clear();
            input.put("ia", 2.0);
            input.put("ib", 20.0);
            input.put("ic", 500.0);
            input.put("id", 1000.0);
            sentval += 4;
            nvnode.process(input);
        }
        nvnode.endWindow();
        ival = 0;
      if (classifySink.dohash) {
        for (Map.Entry<String, Integer> e: classifySink.collectedTuples.entrySet()) {
          ival += e.getValue().intValue();
        }
      }
      else {
        ival = classifySink.count;
      }
        LOG.info(String.format("\nThe number of keys in %d tuples are %d and %d",
                ival,
                classifySink.collectedTuples.size(),
                classifySink.collectedTupleValues.size()));
        for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
            Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
            Log.info(String.format("%d tuples of key \"%s\" has value %f",
                    ieval.intValue(),
                    ve.getKey(),
                    ve.getValue()));
        }
        Assert.assertEquals("number emitted tuples", sentval, ival);
    }
}
