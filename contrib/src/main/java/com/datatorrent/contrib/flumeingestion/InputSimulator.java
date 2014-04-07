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

import java.io.*;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.ShipContainingJars;

import com.datatorrent.lib.bucket.Bucketable;

@ShipContainingJars(classes = {DateTimeFormat.class, Bucketable.class})
public class InputSimulator extends BaseOperator implements InputOperator
{
  private int rate;
  private int percentPastEvents;
  private String filePath;

  public final transient DefaultOutputPort<FlumeEvent> output = new DefaultOutputPort<FlumeEvent>();
  private transient List<FlumeEvent> cache;
  private transient int numberOfPastEvents;
  private transient int startIndex;
  private transient int cacheSize;
  private transient Random random;

  public InputSimulator()
  {
    rate = 2500;
    percentPastEvents = 5;
    random = new Random();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      BufferedReader lineReader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
      try {
        buildCache(lineReader);
      }
      finally {
        lineReader.close();
      }
    }
    catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    cacheSize = cache.size();
    numberOfPastEvents = percentPastEvents / 100 * cacheSize;
    logger.debug("config {} {} {}", cacheSize, rate, numberOfPastEvents);
  }

  private void buildCache(BufferedReader lineReader) throws IOException
  {
    cache = Lists.newArrayListWithCapacity(rate);

    String line;
    while ((line = lineReader.readLine()) != null) {
      FlumeEvent event = FlumeEvent.from(line.getBytes(), Application.FIELD_SEPARATOR);
      cache.add(event);
    }
  }

  @Override
  public void emitTuples()
  {
    int lastIndex = startIndex + rate;
    if (lastIndex > cacheSize) {
      lastIndex -= cacheSize;
      processBatch(startIndex, cacheSize);
      startIndex = 0;
      while (lastIndex > cacheSize) {
        processBatch(0, cacheSize);
        lastIndex -= cacheSize;
      }
      processBatch(0, lastIndex);
    }
    else {
      processBatch(startIndex, lastIndex);
    }
    startIndex = lastIndex;
    logger.debug("emit {}", System.currentTimeMillis());
  }

  private void processBatch(int start, int end)
  {
    logger.debug("process {} {}", start, end);
    int total = end - start;
    if (total <= 0) {
      return;
    }

    int noise = random.nextInt(numberOfPastEvents + 1);
    Set<Integer> pastIndices = Sets.newHashSet();
    for (int i = 0; i < noise; i++) {
      pastIndices.add(random.nextInt(total));
    }

    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DATE, -1);

    for (int i = start; i < end; i++) {
      FlumeEvent eventRow = cache.get(i);

      if (pastIndices.contains(i)) {
        eventRow.time = calendar.getTimeInMillis();
      }
      else {
        eventRow.time = System.currentTimeMillis();
        byte randomByte = (byte) random.nextInt(128);
        int idIndex = eventRow.id.offset + random.nextInt(eventRow.id.length);
        eventRow.id.buffer[idIndex] = randomByte;
      }
      output.emit(eventRow);
    }
  }

  /**
   * @param rate the rate per streaming window at which to consume the lines from the file.
   */
  public void setRate(int rate)
  {
    this.rate = rate;
  }

  public void setFilePath(String path)
  {
    this.filePath = path;
  }

  public void setPercentPastEvents(int percentPastEvents)
  {
    this.percentPastEvents = percentPastEvents;
  }

  private static final Logger logger = LoggerFactory.getLogger(InputSimulator.class);

}
