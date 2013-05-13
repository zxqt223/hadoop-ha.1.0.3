package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

public class FSEditLogTest extends TestCase{

  private EditLogInputStream edits=null;
  private String EDITS_FILE_NAME="src/contrib/ha/src/resources/edits";
//  private String IMAGE_FILE_NAME="src/contrib/ha/src/resources/fsimage";
  

  public void setUp(){
     try {
    	File editsFile=new File(EDITS_FILE_NAME);
		edits=new FSEditLog.EditLogFileInputStream(editsFile);
//		FSImage fsImage = new FSImage(new File(IMAGE_FILE_NAME));
//		FSNamesystem fsns=new FSNamesystem(fsImage , new Configuration());
	} catch (IOException e) {
		e.printStackTrace();
	}
  }
  
  public void testLoadFSEdits(){
	try {
	  FSEditLog.loadFSEdits(edits);
    } catch (IOException e) {
      fail(e.getMessage());
	  e.printStackTrace();
	}
  }
  
}
