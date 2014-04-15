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

import java.io.*;
import java.util.List;
import java.util.Random;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
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
public class HdfsInputOperator implements InputOperator
{
  public static final byte FIELD_SEPARATOR = 2;

  @OutputPortFieldAnnotation(name = "output")
  public final transient DefaultOutputPort<FlumeEvent> output = new DefaultOutputPort<FlumeEvent>();

  private String directory;
  private int rate;
  private transient int lineCount;

  private transient FileSystem fs;
  private transient Path directoryPath;
  private transient List<Path> files;
  private transient int currentFile = 0;
  private transient BufferedReader br = null;
  private transient Random random;

  public HdfsInputOperator()
  {
    rate = 2500;
    files = Lists.newArrayList();
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      directoryPath = new Path(directory);
      fs = FileSystem.get(directoryPath.toUri(), new Configuration());
      findFiles();
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

  private void openNextFile()
  {
    Path filePath = files.get(currentFile);
    currentFile = (currentFile + 1) % files.size();

    logger.info("opening file {}", filePath);

    InputStream input;
    try {
      input = fs.open(filePath);
      if (filePath.toString().endsWith(".gz")) {
        input = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP, input);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    catch (CompressorException e) {
      throw new RuntimeException(e);
    }
    br = new BufferedReader(new InputStreamReader(input));
  }

  private void findFiles()
  {
    try {
      logger.debug("checking for new files in {}", directoryPath);
      RemoteIterator<LocatedFileStatus> statuses = fs.listFiles(directoryPath, true);
      for (; statuses.hasNext(); ) {
        FileStatus status = statuses.next();
        Path path = status.getPath();
        String filePathStr = path.toString();
        if (!filePathStr.endsWith(".gz")) {
          continue;
        }
        logger.debug("new file {}", filePathStr);
        this.files.add(path);
      }
    }
    catch (FileNotFoundException e) {
      logger.warn("Failed to list directory {}", directoryPath, e);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    lineCount = 0;
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void emitTuples()
  {
    if (lineCount >= rate) {
      return;
    }
    while (lineCount < rate) {
      if (br == null) {
        openNextFile();
        continue;
      }
      String line;
      try {
        line = br.readLine();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (line == null) {
        openNextFile();
        continue;
      }
      lineCount++;
      emitLine(line);
    }
  }

  private void emitLine(String line)
  {
    FlumeEvent event = FlumeEvent.from(line.getBytes(), FIELD_SEPARATOR, 43);
    event.time = System.currentTimeMillis();
    output.emit(event);
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

}
