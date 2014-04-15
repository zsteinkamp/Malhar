/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.flumeingestion;

import junit.framework.Assert;
import org.junit.Test;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class HdfsInputOperatorTest
{
  @Test
  public void test(){
    HdfsInputOperator input = new HdfsInputOperator();
    input.setRate(10);
    input.setDirectory("resources/test_data");
    input.setup(new OperatorContextTestHelper.TestIdOperatorContext(0));
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    input.output.setSink(sink);

    input.beginWindow(0);
    input.emitTuples();
    input.endWindow();
    Assert.assertEquals(10,sink.collectedTuples.size());
    sink.clear();

    input.beginWindow(1);
    input.emitTuples();
    input.endWindow();
    Assert.assertEquals(10, sink.collectedTuples.size());
    sink.clear();
  }
}
