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

import java.io.Serializable;
import java.util.Arrays;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.annotation.ShipContainingJars;

import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.bucket.Bucketable;
import com.datatorrent.lib.bucket.Event;

@ShipContainingJars(classes = {DateTimeFormat.class})
public class FlumeEvent implements Bucketable, Event
{
  public Slice id;
  public long time;
  public int dimensionsOffset;

  public Slice getId()
  {
    return id;
  }

  @Override
  public long getTime()
  {
    return time;
  }

  FlumeEvent()
  {
    /* needed for Kryo serialization */
  }

  public static FlumeEvent from(byte[] row, byte separator)
  {
    final int rowsize = row.length;

    /*
     * Lets get the id out of the current record
     */
    int sliceLengh = -1;
    while (++sliceLengh < rowsize) {
      if (row[sliceLengh] == separator) {
        break;
      }
    }

    /* skip the next record */
    int i = sliceLengh + 1;
    while (i < rowsize) {
      if (row[i++] == separator) {
        break;
      }
    }

    /* lets parse the date */
    int dateStart = i;
    while (i < rowsize) {
      if (row[i++] == separator) {
        FlumeEvent event = new FlumeEvent();
        event.id = new Slice(row, 0, sliceLengh);
        event.time = DATE_PARSER.parseMillis(new String(row, dateStart, i - dateStart - 1));
        event.dimensionsOffset = i;
        return event;
      }
    }

    return null;
  }

  @Override
  public int hashCode()
  {
    int hash = 5;
    hash = 61 * hash + (this.id != null ? this.id.hashCode() : 0);
    hash = 61 * hash + (int) (this.time ^ (this.time >>> 32));
    return hash;
  }

  @Override
  public String toString()
  {
    return "FlumeEvent{" + "id=" + id + ", time=" + time + '}';
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final FlumeEvent other = (FlumeEvent) obj;
    if (this.id != other.id && (this.id == null || !this.id.equals(other.id))) {
      return false;
    }
    return this.time == other.time;
  }

  @Override
  public Object getEventKey()
  {
    return new EventKey(Arrays.copyOfRange(id.buffer, id.offset, id.offset + id.length), time);

  }

  public static class EventKey implements Serializable
  {
    final byte[] id;
    final long time;

    private EventKey()
    {
      /* for serialization purpose */
      this(null, 0);
    }

    public EventKey(byte[] id, long time)
    {
      this.id = id;
      this.time = time;
    }

    @Override
    public int hashCode()
    {
      int hash = 3;
      hash = 59 * hash + Arrays.hashCode(this.id);
      hash = 59 * hash + (int) (this.time ^ (this.time >>> 32));
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final EventKey other = (EventKey) obj;
      if (!Arrays.equals(this.id, other.id)) {
        return false;
      }
      if (this.time != other.time) {
        return false;
      }
      return true;
    }

    private static final long serialVersionUID = 201404021744L;
  }

  static final DateTimeFormatter DATE_PARSER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
  private static final Logger logger = LoggerFactory.getLogger(FlumeEvent.class);
}
