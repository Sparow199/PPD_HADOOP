import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.Iterator;
import java.util.Arrays;


public class Poste {

  //class de mappage hadoop
  public static class TokenizerMapper extends Mapper<Object,Text,Text,Text>{

    private Text word = new Text();

    //methode de mappage hadoop
    public void map(Object key,Text value, Context context) throws IOException,InterruptedException{


      String ville = value.toString().split(";")[8].toString();

      String corgps = value.toString().split(";")[10].toString();

      context.write(new Text(ville),new Text(corgps));
    }
  }

  //class de reduction hadoop
  public static class PosteReducer extends Reducer<Text,Text,Text,Text>{

    // methode de réduction
    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{

      int compt = 0;
      Iterator<Text> iter = values.iterator();
      StringBuilder maker = new StringBuilder();
      maker.append(key);

      while(iter.hasNext()) {
        Text coords = iter.next();
        compt ++;
        maker.append(coords);
        maker.append(";");
      }
        context.write(new Text(key.toString() +"("+ compt +")"),new Text(maker.toString()));
    }
  }

  //methode main 
  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "poste");
    job.setJarByClass(Poste.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(PosteReducer.class);
    job.setReducerClass(PosteReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
