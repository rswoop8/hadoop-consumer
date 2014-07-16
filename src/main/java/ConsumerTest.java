import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.*;

public class ConsumerTest implements Runnable {
	private KafkaStream m_stream;
	public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
    m_stream = a_stream;
  }

	public void run() {
    ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
    
    FileWriter writer = null;
    BufferedWriter buffer = null;
    FileWriter writer2 = null;
    BufferedWriter buffer2 = null;
    
    int numMessages = 1;
    int fileNumber = 0;
    
    //Initialize directory
    File directory = new File(System.getProperty("user.home") + "/kafka");
    
    //Check if directory exists, if not, make it
    if (!directory.exists() || directory.isFile()) {
    	
    	try {
        directory.mkdir();
      }
    	
    	catch(SecurityException se) {
    		System.out.println("Failed to make directory");
      }    
    }
    
    //Create files
    File file = new File(directory.getAbsolutePath() +
      "/KafkaTopic" + RunConsumer.topic + fileNumber + ".txt");
    File logfile = new File(directory.getAbsolutePath() +
      "/outputfilelogtopic" + RunConsumer.topic + ".txt");
    if (logfile.isFile()) {
    	BufferedReader input = null;
    	
			try {
				input = new BufferedReader(new FileReader(directory.
				  getAbsolutePath() + "/outputfilelogtopic" +
				  RunConsumer.topic + ".txt"));
			}
			
			catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			
      String line;
      try {
      	
				while ((line = input.readLine()) != null) {
				  fileNumber = Integer.parseInt(line);
				}
			}
      
      catch (NumberFormatException e) {
				e.printStackTrace();
			}
      
      catch (IOException e) {
				e.printStackTrace();
			}
    }
    
    else {
    	try {
        //Create FileWriter and BufferedWriter, set FileWriter append to true
        writer2 = new FileWriter(logfile, true);
        buffer2 = new BufferedWriter(writer2);

        //Write to file
        buffer2.write("0");
      }

      catch (IOException e) {
        e.printStackTrace();
      }

      finally {
        try {
          buffer2.close();
          writer2.close();
        }

        catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    file = new File(directory.getAbsolutePath() + "/KafkaTopic" +
      RunConsumer.topic + fileNumber + ".txt");
    
    while (it.hasNext()) {
      if (numMessages <= 1000) {
        String msg = new String(it.next().message());
        System.out.println("Topic '" + RunConsumer.topic + "' message "
          + numMessages + ": " + msg);

        try {
          //Create FileWriter and BufferedWriter, set FileWriter append to true
          writer = new FileWriter(file, true);
          buffer = new BufferedWriter(writer);
          
          writer2 = new FileWriter(logfile, true);
          buffer2 = new BufferedWriter(writer2);

          //Write to file
          buffer.write(msg);
          buffer.write(System.getProperty("line.separator"));
        }

        catch (IOException e) {
          e.printStackTrace();
        }

        finally {
          try {
            buffer.close();
            writer.close();
          }

          catch (IOException e) {
            e.printStackTrace();
          }
        }
        //Increment message received count
        numMessages++;
      }

      else {
        numMessages = 1;
        try {
          FileSystem hdfs=FileSystem.get(new Configuration());

          //Get home directory
          Path homeDir=hdfs.getHomeDirectory();
          //Set HDFS path
          Path newFolderPath= new Path(homeDir + "/kafka");
          //Set local path
          if(!hdfs.exists(newFolderPath)) {
              hdfs.mkdirs(newFolderPath);
          }
          Path localFilePath = new Path(directory.getAbsolutePath() +
            "/KafkaTopic" + RunConsumer.topic + fileNumber + ".txt");
          //Set HDFS file location
          Path hdfsFilePath=new Path(newFolderPath + "/KafkaTopic" +
            RunConsumer.topic + fileNumber + ".txt");

          hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);
        }

        catch (Exception ex) {
          Thread t = Thread.currentThread();
          t.getUncaughtExceptionHandler().uncaughtException(t, ex);
        }
        fileNumber++;
        try {
          //Create FileWriter and BufferedWriter, set FileWriter append to true
          writer2 = new FileWriter(logfile, true);
          buffer2 = new BufferedWriter(writer2);

          //Write to file
          buffer2.write(System.getProperty("line.separator"));
          buffer2.write(Integer.toString(fileNumber));
        }

        catch (IOException e) {
          e.printStackTrace();
        }

        finally {
          try {
            buffer2.close();
            writer2.close();
          }

          catch (IOException e) {
            e.printStackTrace();
          }
        }
        file.delete();
        run();
      }
    }
  }
}