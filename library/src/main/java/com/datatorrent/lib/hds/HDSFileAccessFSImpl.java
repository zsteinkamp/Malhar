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
package com.datatorrent.lib.hds;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.TreeMap;

import org.apache.commons.io.output.CountingOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Hadoop file system backed store.
 */
public class HDSFileAccessFSImpl implements HDSFileAccess
{
  private final FileSystem fs;
  private final String basePath;

  public HDSFileAccessFSImpl(FileSystem fs, String basePath)
  {
    this.fs = fs;
    this.basePath = basePath;
  }

  protected Path getBucketPath(long bucketKey)
  {
    return new Path(basePath, Long.toString(bucketKey));
  }

  @Override
  public void close() throws IOException
  {
  }

  @Override
  public void init()
  {
  }

  @Override
  public void delete(long bucketKey, String fileName) throws IOException
  {
    fs.delete(new Path(getBucketPath(bucketKey), fileName), true);
  }

  @Override
  public DataOutputStream getOutputStream(long bucketKey, String fileName) throws IOException
  {
    Path path = new Path(getBucketPath(bucketKey), fileName);
    if (!fs.exists(path)) {
      return fs.create(path);
    }
    return fs.append(path);
  }

  @Override
  public DataInputStream getInputStream(long bucketKey, String fileName) throws IOException
  {
    return fs.open(new Path(getBucketPath(bucketKey), fileName));
  }

  @Override
  public void rename(long bucketKey, String fromName, String toName) throws IOException
  {
    FileContext fc = FileContext.getFileContext(fs.getUri());
    Path bucketPath = getBucketPath(bucketKey);
    fc.rename(new Path(bucketPath, fromName), new Path(bucketPath, toName), Rename.OVERWRITE);
  }

  private final transient Kryo kryo = new Kryo();

  @Override
  public HDSFileReader getReader(final long bucketKey, final String fileName) throws IOException
  {
    return new HDSFileReader() {
      @Override
      public void close() throws IOException
      {
      }

      @Override
      public void readFully(TreeMap<byte[], byte[]> data) throws IOException
      {
        InputStream is = getInputStream(bucketKey, fileName);
        Input input = new Input(is);
        while (!input.eof()) {
          byte[] key = kryo.readObject(input, byte[].class);
          byte[] value = kryo.readObject(input, byte[].class);
          data.put(key, value);
        }
      }
    };
  }

  @Override
  public HDSFileWriter getWriter(final long bucketKey, final String fileName) throws IOException
  {
    final DataOutputStream dos = getOutputStream(bucketKey, fileName);
    final CountingOutputStream cos = new CountingOutputStream(dos);
    final Output out = new Output(cos);

    return new HDSFileWriter() {
      @Override
      public void close() throws IOException
      {
        out.close();
        cos.close();
        dos.close();
      }

      @Override
      public void append(byte[] key, byte[] value) throws IOException
      {
        kryo.writeObject(out, key);
        kryo.writeObject(out, value);
      }

      @Override
      public int getFileSize()
      {
        return cos.getCount() + out.position();
      }

    };

  }

}