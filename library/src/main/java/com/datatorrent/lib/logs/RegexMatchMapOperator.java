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
package com.datatorrent.lib.logs;

import java.util.HashMap;
import java.util.Map;
import com.google.code.regexp.Pattern;
import com.google.code.regexp.Matcher;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.Stateless;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use this operator to parse unstructured log data into named fields.
 *
 * Uses a regex with [named capturing groups](http://www.regular-expressions.info/named.html) to extract portions of a string read
 * from the input port into a `Map<String,String>`. The capturing group name is used as the key name. The captured value is used
 * as the value.
 *
 * For example, given the input:
 *
 * ```
 * 12345 "foo bar" baz;goober
 * ```
 *
 * And the regular expression:
 *
 * ```
 * (?<id>\d+) "(?<username>[^"]+)" (?<action>[^;]+);(?<cookie>.+)
 * ```
 *
 * The operator would emit a `Map` containing:
 *
 * key      | val
 * ---------|--------
 * id       | 12345
 * username | foo bar
 * action   | baz
 * cookie   | goober
 * 
 * In the case where the regex does not match the input, nothing is emitted.
 *
 * Uses the named-regexp library originally from Google, but now maintained
 * by [Anthony Trinh](https://github.com/tony19/named-regexp).
 *
 * This is a passthrough operator.
 *
 * * __Stateful__ : No
 * * __Partitions__ : Yes, No dependency among input values.
 * * __Ports__
 *     * __data:__ expects String
 *     * __output:__ emits Map
 * * __Properties__
 *     * __regex:__ defines the regex (must use named capturing groups)
 *
 * Here is an example UML diagram
 * ==============================
 * ![Example Diagram](example.png)
 *
 * @uml example.png
 * Alice -> Bob: Authentication Request
 * Bob --> Alice: Authentication Response
 *
 * @since 1.0.5 */
@Stateless
public class RegexMatchMapOperator extends BaseOperator
{
  /**
   * The regex string
   */
  private String regex = null;
  private transient Pattern pattern = null;
  /**
   * Input log line port.
   */
  public final transient DefaultInputPort<String> data = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s) throws RuntimeException
    {
      processTuple(s);
    }

  };
  /**
   * Output port.
   */
  public final transient DefaultOutputPort<Map<String, Object>> output = new DefaultOutputPort<Map<String, Object>>();

  /**
   * @return the regex
   */
  public String getRegex()
  {
    return regex;
  }

  /**
   * @param regex
   * the regex to set
   */
  public void setRegex(String regex)
  {
    this.regex = regex;
    pattern = Pattern.compile(this.regex);
  }

  /**
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if (this.regex != null) {
      pattern = Pattern.compile(this.regex);
    }
  }

  /**
   * Parses string with regex, and emits a Map corresponding to named capturing group names and their captured values.
   *
   * @param line tuple to parse
   * @throws RuntimeException
   */
  public void processTuple(String line) throws RuntimeException
  {
    if (pattern == null) {
      throw new RuntimeException("regex has not been set");
    }

    Matcher matcher = pattern.matcher(line);
    if (matcher.matches()) {
      Map<String, Object> outputMap = new HashMap<String, Object>();

      for (String key : pattern.groupNames()) {
        outputMap.put(key, matcher.group(key));
      }
      output.emit(outputMap);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(RegexMatchMapOperator.class);

}
