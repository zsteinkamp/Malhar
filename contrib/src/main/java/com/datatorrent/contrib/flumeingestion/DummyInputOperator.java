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

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.annotation.ShipContainingJars;

import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

@ShipContainingJars(classes = {CompressorStreamFactory.class})
public class DummyInputOperator implements InputOperator
{
  public static final byte FIELD_SEPARATOR = 2;

  @OutputPortFieldAnnotation(name = "output")
  public final transient DefaultOutputPort<FlumeEvent> output = new DefaultOutputPort<FlumeEvent>();

  private String directory="testDir";
  private int rate;
  private transient int lineCount;

  private transient FileSystem fs;
  private transient FileSystem anotherFs;
  private transient Path directoryPath;
  private transient List<Path> files;
  private transient int currentFile = 0;
  private transient BufferedReader br = null;
  private transient Random random;

  public DummyInputOperator()
  {
    rate = 2500;
    files = Lists.newArrayList();
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      directoryPath = new Path(directory);
      fs = FileSystem.newInstance(directoryPath.toUri(), new Configuration());
      anotherFs = FileSystem.newInstance(directoryPath.toUri(), new Configuration());
      fs.close();
      anotherFs.create(new Path("temp"));
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    if (br != null) {
      try {
        br.close();
      }
      catch (Exception e) {
        // ignore
      }
    }
  }

  public void setRate(int rate)
  {
    this.rate = rate;
  }


  @Override
  public void emitTuples()
  {

  }

  private void emitLine(String line)
  {

  }

  public String getDirectory()
  {
    return directory;
  }

  public void setDirectory(String directory)
  {
    this.directory = directory;
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractFileInputOperator.class);

  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {

  }
}
