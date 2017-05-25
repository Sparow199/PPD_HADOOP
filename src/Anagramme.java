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


public class Anagramme {

  //class de mappage hadoop
  public static class TokenizerMapper extends Mapper<Object,Text,Text,Text>{
    private Text word = new Text();

    //methode de mappage
    public void map(Object key,Text value, Context context) throws IOException,InterruptedException{

      char[] text = value.toString().toLowerCase().toCharArray();

      //tri la chaine par ordre alphabetique

      Arrays.sort(text);

      //la clé => mot Sorté, valeur => mot de base
      context.write(new Text(new String(text)),value);
    }
  }

  // class de réduction hadoop
  public static class AnagrammeReducer extends Reducer<Text,Text,Text,Text>{

    //methode de reduction
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{

      Iterator<Text> iter = values.iterator();
      boolean firstWord = true;
      StringBuilder maker = new StringBuilder();

      while(iter.hasNext()){

        Text currentWord = iter.next();

        if(firstWord) {
          firstWord = false;
          maker.append(currentWord);
        } else {
          maker.append(";" + currentWord);
        }

      }
      context.write(key,new Text(maker.toString()));
    }
  }

  //methode main 
  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "anagramme");
    job.setJarByClass(Anagramme.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(AnagrammeReducer.class);
    job.setReducerClass(AnagrammeReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
