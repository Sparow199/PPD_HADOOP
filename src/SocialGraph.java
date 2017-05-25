import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class SocialGraph {

  //class de mappage hadoop
  public static class UserMapper
  extends Mapper<Object, Text,Text, Text> {

    //methode de mappage
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(" ");
      String result;
      IntWritable first = new IntWritable();
      IntWritable second = new IntWritable();

      for(int i = 1; i < line.length; i++) {
        result = new String();

        if(Integer.parseInt(line[0]) > Integer.parseInt(line[i])) {
          first = new IntWritable(Integer.parseInt(line[i]));
          second = new IntWritable(Integer.parseInt(line[0]));
        }
        if(Integer.parseInt(line[0]) < Integer.parseInt(line[i])) {
          first = new IntWritable(Integer.parseInt(line[0]));
          second = new IntWritable(Integer.parseInt(line[i]));
        }


        for(int j = 1; j < line.length; j++) {
          if(j != i) {
            result += line[j] + "|";
          }
        }

        context.write(new Text(new String("("+first.get()+", "+second.get()+")")), new Text(result));
      }
    }
  }

  // class de réduction hadoop
  public static class FriendReducer
  extends Reducer<Text, Text, Text, Text> {

        //methode de reduction
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Iterator<Text> iter = values.iterator();
      Text current, next;
      ArrayList<String> result;
      boolean first = true;
      int nbFriends;
      current = new Text();
      next = new Text();

      while(iter.hasNext()) {
        result = new ArrayList<String>();
        nbFriends = 0;
        if(first) {
          current = iter.next();
          next = iter.next();
          first = false;
        }

        String[] csplit = current.toString().split("|");
        String[] nsplit = next.toString().split("|");

        for(int j = 0; j < csplit.length; j++) {
          for(int k = 0; k < nsplit.length; k++) {
            if(csplit[j].equals(nsplit[k]) && (!csplit[j].isEmpty()) && (!nsplit[k].isEmpty())) {
              if(!result.contains(csplit[j])) {
                result.add(csplit[j]);
                nbFriends++;
              }
            }
          }
        }

        StringBuilder finalresult = new StringBuilder();
        for(int j = 0; j < result.size(); ++j) {
          finalresult.append(result.get(j) + "|");
        }
        context.write(key, new Text(new String("FriendNumber: " + nbFriends+" Friends: "+finalresult)));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "social graph");
    job.setJarByClass(SocialGraph.class);
    job.setMapperClass(UserMapper.class);
    job.setReducerClass(FriendReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
