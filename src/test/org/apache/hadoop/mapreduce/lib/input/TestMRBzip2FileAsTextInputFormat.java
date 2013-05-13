/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.input;

import java.io.*;
import java.util.*;

import junit.framework.TestCase;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class TestMRBzip2FileAsTextInputFormat extends TestCase {
	private static final Log LOG =
	    LogFactory.getLog(TestMRBzip2FileAsTextInputFormat.class.getName());

	  private static int MAX_LENGTH = 10000;
	  
	  private static Configuration defaultConf = new Configuration();
	  private static FileSystem localFs = null; 
	  static {
	    try {
	      localFs = FileSystem.getLocal(defaultConf);
	    } catch (IOException e) {
	      throw new RuntimeException("init failure", e);
	    }
	  }
	  private static Path inputDir = 
	    new Path("/Users/zhangscott/Downloads/data/input");
	  private static Path outputDir = 
		    new Path("/Users/zhangscott/Downloads/data/output4");
	  
	  public void testFormat() throws Exception {
	    Job job = new Job(defaultConf);

	    FileInputFormat.setInputPaths(job, inputDir);
	    
//	    int seed = new Random().nextInt();
//	    LOG.info("seed = "+seed);
//	    Random random = new Random(seed);
//
//	    localFs.delete(workDir, true);
//	    
//
//	    // for a variety of lengths
//	    for (int length = 0; length < MAX_LENGTH;
//	         length+= random.nextInt(MAX_LENGTH/10)+1) {
//
//	      LOG.debug("creating; entries = " + length);
//
//	      // create a file with length entries
//	      Writer writer = new OutputStreamWriter(localFs.create(file));
//	      try {
//	        for (int i = 0; i < length; i++) {
//	          writer.write(Integer.toString(i*2));
//	          writer.write("\t");
//	          writer.write(Integer.toString(i));
//	          writer.write("\n");
//	        }
//	      } finally {
//	        writer.close();
//	      }

	      TextInputFormat format = new TextInputFormat();
	      JobContext jobContext = new JobContext(job.getConfiguration(), new JobID());
	      List<InputSplit> splits = format.getSplits(jobContext);
	      LOG.info("splitting: got =        " + splits.size());
	      
	      TaskAttemptContext context = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID());

	      // check each split
	      //BitSet bits = new BitSet(length);
	      int count = 0;
	      int i=0;
	      for (InputSplit split : splits) {
	        LOG.info("split= " + split);
	        RecordReader<LongWritable, Text> reader =
	          format.createRecordReader(split, context);
	        Class readerClass = reader.getClass();
	        assertEquals("reader class is KeyValueLineRecordReader.", LineRecordReader.class, readerClass);        

	        reader.initialize(split, context);
		    Path ouputFile = new Path(outputDir, "out_"+i++);

	        Writer writer = new OutputStreamWriter(localFs.create(ouputFile));
	        try {
	          while (reader.nextKeyValue()) {
	            System.out.println("key="+reader.getCurrentKey().toString()+",value="+reader.getCurrentValue().toString());
	            writer.write(reader.getCurrentValue().toString()+"\n");
	            count++;
	          }
	          LOG.debug("split="+split+" count=" + count);
	        } finally {
	          reader.close();
	          writer.close();
	        }
	      }
	      
	      System.out.println("total count = "+ count);
//	    }
	  }
	  private LineReader makeStream(String str) throws IOException {
	    return new LineReader(new ByteArrayInputStream
	                                           (str.getBytes("UTF-8")), 
	                                           defaultConf);
	  }
	  
	  public void testUTF8() throws Exception {
	    LineReader in = makeStream("abcd\u20acbdcd\u20ac");
	    Text line = new Text();
	    in.readLine(line);
	    assertEquals("readLine changed utf8 characters", 
	                 "abcd\u20acbdcd\u20ac", line.toString());
	    in = makeStream("abc\u200axyz");
	    in.readLine(line);
	    assertEquals("split on fake newline", "abc\u200axyz", line.toString());
	  }

	  public void testNewLines() throws Exception {
	    LineReader in = makeStream("a\nbb\n\nccc\rdddd\r\neeeee");
	    Text out = new Text();
	    in.readLine(out);
	    assertEquals("line1 length", 1, out.getLength());
	    in.readLine(out);
	    assertEquals("line2 length", 2, out.getLength());
	    in.readLine(out);
	    assertEquals("line3 length", 0, out.getLength());
	    in.readLine(out);
	    assertEquals("line4 length", 3, out.getLength());
	    in.readLine(out);
	    assertEquals("line5 length", 4, out.getLength());
	    in.readLine(out);
	    assertEquals("line5 length", 5, out.getLength());
	    assertEquals("end of file", 0, in.readLine(out));
	  }
	  
	  private static void writeFile(FileSystem fs, Path name, 
	                                CompressionCodec codec,
	                                String contents) throws IOException {
	    OutputStream stm;
	    if (codec == null) {
	      stm = fs.create(name);
	    } else {
	      stm = codec.createOutputStream(fs.create(name));
	    }
	    stm.write(contents.getBytes());
	    stm.close();
	  }
	  
	  private static List<Text> readSplit(TextInputFormat format, 
	                                      InputSplit split, 
	                                      TaskAttemptContext context) throws IOException, InterruptedException {
	    List<Text> result = new ArrayList<Text>();
	    RecordReader<LongWritable, Text> reader = format.createRecordReader(split, context);
	    reader.initialize(split, context);
	    while (reader.nextKeyValue()) {
	      result.add(new Text(reader.getCurrentValue()));
	    }
	    return result;
	  }
	
	  public static void main(String[] args) throws Exception {
	    new TestMRBzip2FileAsTextInputFormat().testFormat();
	  }
}
