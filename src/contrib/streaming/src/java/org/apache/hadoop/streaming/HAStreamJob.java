package org.apache.hadoop.streaming;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobClient;

/**
 * add the high availability of the streaming job
 * 
 * @author Zhancheng Deng {@mailto: zhancheng.deng@renren-inc.com}
 * @since 5:59:04 PM Jan 4, 2013
 */
public class HAStreamJob extends StreamJob {
  /**
   * @ADDCODE override the base class (StreamJob) to function the high
   *          availability of the mapreduce.
   */
  @Override
  public int submitAndMonitorJob() throws IOException {

    if ((jar_ != null && isLocalHadoop())
        || jobConf_.get("jobtracker.servers", "").equals("")) {
      return super.submitAndMonitorJob();
    }

    // if jobConf_ changes must recreate a JobClient
    jc_ = new JobClient(jobConf_);
    running_ = null;
    boolean error = true;
    int ret = 0;
    try {
      running_ = jc_.submitJob(jobConf_);
      jobId_ = running_.getID();
      LOG.info("getLocalDirs(): " + Arrays.asList(jobConf_.getLocalDirs()));
      LOG.info("Running job: " + jobId_);
      jobInfo();
      try {
        ret = printJobStatus();
      } catch (IOException e) {
        long sleeptime = jc_.getConf().getLong(
            "mapred.submit.highavailability.interval", 30 * 1000);
        LOG.info("Job connection failed once, sleep " + sleeptime
            + "ms to reconnect the failover jobtracker node. ");
        try {
          Thread.sleep(sleeptime);
        } catch (InterruptedException e1) {
        }
        ret = printJobStatus();
      }

      LOG.info("Job complete: " + jobId_);
      LOG.info("Output: " + output_);
      error = false;

    } catch (FileNotFoundException fe) {
      LOG.error("Error launching job , bad input path : " + fe.getMessage());
      return 2;
    } catch (InvalidJobConfException je) {
      LOG.error("Error launching job , Invalid job conf : " + je.getMessage());
      return 3;
    } catch (FileAlreadyExistsException fae) {
      LOG.error("Error launching job , Output path already exists : "
          + fae.getMessage());
      return 4;
    } catch (IOException ioe) {
      LOG.error("Error Launching job : " + ioe.getMessage());
      return 5;

    } finally {
      if (error && (running_ != null)) {
        LOG.info("killJob...");
        running_.killJob();
      }
      jc_.close();
    }
    return ret;
  }

  private int printJobStatus() throws IOException {
    String lastReport = null;
    while (!running_.isComplete()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      running_ = jc_.getJob(jobId_);
      String report = null;
      report = " map " + Math.round(running_.mapProgress() * 100)
          + "%  reduce " + Math.round(running_.reduceProgress() * 100) + "%";
      if (!report.equals(lastReport)) {
        LOG.info(report);
        lastReport = report;
      }
    }
    if (!running_.isSuccessful()) {
      jobInfo();
      LOG.error("Job not successful. Error: " + running_.getFailureInfo());
      return 1;
    }
    return 0;
  }
}
