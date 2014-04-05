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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.api.StreamCodec;

import com.datatorrent.common.util.Slice;

public class FlumeEventCodec implements StreamCodec<Event>
{
  private transient final Kryo kryo;

  public FlumeEventCodec()
  {
    this.kryo = new Kryo();
    this.kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
  }

  @Override
  public Object fromByteArray(Slice fragment)
  {
    ByteArrayInputStream is = new ByteArrayInputStream(fragment.buffer, fragment.offset, fragment.length);
    Input input = new Input(is);

    @SuppressWarnings("unchecked")
    byte[] body = kryo.readObject(input, byte[].class);
    return EventBuilder.withBody(body);
  }

  @Override
  public Slice toByteArray(Event event)
  {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Output output = new Output(os);
    kryo.writeObject(output, event.getBody());
    output.flush();
    final byte[] bytes = os.toByteArray();
    return new Slice(bytes, 0, bytes.length);
  }

  @Override
  public int getPartition(Event o)
  {
    return o.hashCode();
  }

  private static final Logger logger = LoggerFactory.getLogger(FlumeEventCodec.class);
}