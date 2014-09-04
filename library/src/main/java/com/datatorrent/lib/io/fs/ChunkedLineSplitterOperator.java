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
package com.datatorrent.lib.io.fs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * <p>
 This operator implements "tail -f" command. If the operator has reached the end of the file, it will wait till more
 data comes

 <br>
 * <b>Ports</b>:<br>
 * <b>outport</b>: emits &lt;String&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>filePath</b> : Path for file to be read. <br>
 * <b>delimiter</b>: record/line separator character (default='\n').<br>
 * <b>readDelay</b>: Thread sleep interval after failing to read from the file.<br>
 * <b>readFromEnd</b>: if the user wants to start tailing from end of the file.<br>
 * <b>position</b>: The position from where to start reading the file (default=0).<br>
 * <b>numberOfCycles</b>: number of tuples to be emitted in a single emit Tuple call (default=1000).<br>
 * <b>readBytes</b>: number of bytes to attempt to read each time from the file (default=65536).<br>
 * <b>openRetries</b>: number of times to try to open the file in succession (default=10).<br>
 * <b>openRetryInterval</b>: time to wait between open retries (default=100ms).<br>
 * <br>
 *
 * @since 0.9.4
 */
public class ChunkedLineSplitterOperator implements InputOperator, ActivationListener<OperatorContext>
{
  /**
   * The file which will be tailed.
   */
  private String filePath;

  /**
   * This holds the position from which to start reading the stream
   */
  private long position;

  /**
   * The amount of time to wait for the file to be updated after an unsuccessful read 
   * (i.e. already read until the end of the file).
   */
  private long readDelay = 10;

  /**
   * The number of tuples to emit or read attempts in a single window
   */
  private int numberOfCycles = 1000;

  /**
   * Whether to tail from the end or start of file
   */
  private boolean readFromEnd;

  /**
   * The delimiter used to identify the end of a record
   */
  private char delimiter = '\n';

  /**
   * This is used to store the last access time of the file
   */
  private transient long accessTime = -1;

  /**
   * The buffer that holds a chunk of data from the input file
   */
  private String chunk = "";

  /**
   * The length of `chunk`
   */
  private int chunkLength;

  /**
   * The current position inside `chunk` that we are reading from
   */
  private int chunkPos;

  /**
   * The number of bytes to read from the file into `chunk`
   */
  private int readBytes = 65536;

  /**
   * The number of times to retry opening the input file
   */
  private int openRetries = 10;

  /**
   * The time in milliseconds to sleep between `openRetries`
   */
  private int openRetryInterval = 1000;

  private transient RandomAccessFile reader;
  private transient File file;

  /**
   * @return the filePath
   */
  public String getFilePath()
  {
    return filePath;
  }

  /**
   * @param filePath
   *          the filePath to set
   */
  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  /**
   * @return the delimiter
   */
  public char getDelimiter()
  {
    return delimiter;
  }

  /**
   * @param delimiter
   *          the delimiter to set
   */
  public void setDelimiter(char delimiter)
  {
    this.delimiter = delimiter;
  }

  /**
   * @return the readDelay
   */
  public long getReadDelay()
  {
    return readDelay;
  }

  /**
   * @param readDelay
   *          the readDelay to set
   */
  public void setReadDelay(long readDelay)
  {
    this.readDelay = readDelay;
  }

  /**
   * @return the readFromEnd
   */
  public boolean isReadFromEnd()
  {
    return readFromEnd;
  }

  /**
   * @param readFromEnd
   *          the readFromEnd to set
   */
  public void setReadFromEnd(boolean readFromEnd)
  {
    this.readFromEnd = readFromEnd;
  }

  /**
   * @return the position
   */
  public long getPosition()
  {
    return position;
  }

  /**
   * @param position
   *          the position to set
   */
  public void setPosition(long position)
  {
    this.position = position;
  }

  /**
   * @return the numberOfCycles
   */
  public int getNumberOfCycles()
  {
    return numberOfCycles;
  }

  /**
   * @param numberOfCycles
   *          the numberOfCycles to set
   */
  public void setNumberOfCycles(int numberOfCycles)
  {
    this.numberOfCycles = numberOfCycles;
  }

  /**
   * @return the readBytes
   */
  public int getReadBytes()
  {
    return readBytes;
  }

  /**
   * @param bytes
   *          the readBytes to set
   */
  public void setReadBytes(int bytes)
  {
    this.readBytes = bytes;
  }

  /**
   * @return the openRetries
   */
  public int getOpenRetries()
  {
    return openRetries;
  }

  /**
   * @param retries
   *          the openRetries to set
   */
  public void setOpenRetries(int retries)
  {
    this.openRetries = retries;
  }

  /**
   * @return the openRetryInterval
   */
  public int getOpenRetryInterval()
  {
    return openRetryInterval;
  }

  /**
   * @param interval_ms
   *          the openRetryInterval to set
   */
  public void setOpenRetryInterval(int interval_ms)
  {
    this.openRetryInterval = interval_ms;
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    try {
      this.position = reader.getFilePointer();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void activate(OperatorContext ctx)
  {
    try {
      openInputFile(readFromEnd);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deactivate()
  {
    try {
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    position = 0;
  }

  @Override
  /**
   * emit lines read from the input file
   */
  public void emitTuples()
  {
      int cycleCount = 0;
      // loop for `numberOfCycles`, each cycle being either a tuple emitted or an attempt to read from the file
      while (cycleCount++ < numberOfCycles) {
      // try to pull a line from the `chunk`
      String line = null;
      if (chunkLength > 0) {
        line = readLineFromChunk();
      }
      if (line != null) {
        // fetched a line, so emit it
        logger.debug("got a line {}", line);
        output.emit(line);
      } else {
        logger.debug("no line, so trying to read from file");
        // need to read another chunk from the file
        if (!readChunkFromFile()) {
          logger.debug("Didn't read anything from file. Sleeping for {}.", readDelay);
          // failed to read anything, so sleep a bit until the next try
          try {
            Thread.sleep(readDelay);
          } catch (InterruptedException ignored) {
          }
        }
      }
      // get the next piece of data from the input file
    }
  }

  /**
   * Opens the input file for reading, optionally overriding the `position` variable to read from the end of the file.
   * @param startAtEnd whether to start reading from the end of the file
   */
  private void openInputFile(boolean startAtEnd)
  {
    int tries = 0;
    while (tries < openRetries) {
      try {
        file = new File(filePath);
        reader = new RandomAccessFile(file, "r");
        position = startAtEnd ? file.length() : 0;
        reader.seek(position);
        accessTime = System.currentTimeMillis();
        // reset chunk information since we have reopened the file
        chunk = "";
        chunkLength = 0;
        chunkPos = 0;
        logger.debug("Opened file={} position={}", filePath, position);
      } catch (IOException e) {
        // absorb so that we will retry
      }
      if (file.canRead()) {
        // we are done
        return;
      } else {
        // sleep and try again
        try {
          logger.debug("Cannot read from file '{}'. Will retry after sleep.", file);
          Thread.sleep(openRetryInterval);
        } catch (InterruptedException ignored) {
        }
      }
      tries++;
    }
  }

  /**
   * read a line from `chunk`, starting from `chunkPos` up to the next `delimiter`.
   * @return the next line or null
   */
  private String readLineFromChunk()
  {
    int readPos = chunkPos;
    String line = null;
    while (readPos < chunkLength) {
      if (chunk.charAt(readPos) == delimiter) {
        // found delimiter, so grab the string up to the delimiter
        line = chunk.substring(chunkPos, readPos);
        // advance chunkPos past the delimiter
        chunkPos = readPos + 1;
        break;
      } else {
        // advance the `readPos` so we can test the next character
        readPos++;
      }
    }
    return line;
  }

  /**
   * read `readBytes` bytes from the input file and append to the current chunk fragment 
   * (i.e. from `chunkPos` to the end of the chunk).
   *
   * @return whether we read anything from the file
   */
  private boolean readChunkFromFile()
  {
    boolean retval = false;

    try {
      if (!file.canRead()) {
        // throw an exception, which is caught below so that the file will be reopened
        throw new IOException("file not readable");
      }
      long pos = reader.getFilePointer();
      long length = file.length();
      if ((length < pos) || (accessTime >= 0 && length == pos && FileUtils.isFileNewer(file, accessTime))) {
        // throw an exception, which is caught below so that the file will be reopened
        throw new IOException("file has been rotated");
      }

      int bytesToRead = Math.min(readBytes, (int)(reader.length() - position));
      byte[] readbuf;
      if (bytesToRead > 0) {
        logger.debug("Reading {} bytes from file", bytesToRead);
        // bytes are in the file that we haven't seen yet
        readbuf = new byte[bytesToRead];
        // will read up to `readBytes` into `readbuf`
        reader.read(readbuf);
        accessTime = System.currentTimeMillis();
        position = reader.getFilePointer();
        // rebuild the chunk, prepending the existing fragment to the chunk we read from disk
        chunk = chunk.substring(chunkPos) + new String(readbuf);
        chunkPos = 0;
        chunkLength = chunk.length();
        retval = true;
      } else {
        logger.debug("No bytes to read from file {}", file);
      }
    } catch (IOException e) {
      logger.debug("Caught exception [{}]. Reopening input.", e.toString());
      openInputFile(false);
    }
    return retval;
  }

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  private static final Logger logger = LoggerFactory.getLogger(ChunkedLineSplitterOperator.class);

}
