import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.lang.Integer;
import java.lang.Float;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Stats {

   //class de mappage hadoop
   public static class AgeMapper
   extends Mapper<Object, Text, IntWritable, FloatWritable> {

    //methode de mappage hadoop
    public void map(Object key, Text value, Context context) throws IOException,InterruptedException {

      String[] line = value.toString().split(",");
      IntWritable age;
      FloatWritable salary;

      //on ignore les libellés dans notre calcul
      if(line[1].equals("age") || line[4].equals("income")) {
        return;
      }

      int age_value = Integer.parseInt(line[1]);
      float salary_value = Float.parseFloat(line[4]);
      age = new IntWritable();
      salary = new FloatWritable();

      age.set(age_value);
      salary.set(salary_value);

      context.write(age, salary);
    }
   }

  //class de reduction hadoop
  public static class StatsReducer
  extends Reducer<IntWritable, FloatWritable, IntWritable, Text> {

    //methode de reduction hadoop
    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

      int nbSalarie = 0;
      float minSalary = 0, maxSalary = 0, avgSalary = 0, sumSalary = 0;
      double standardDeviation = 0, sumSalaryPow = 0;
      boolean first = true;
      StringBuilder resultat = new StringBuilder();
      Iterator<FloatWritable> iter = values.iterator();
      FloatWritable value;

      while(iter.hasNext()) {
        value = iter.next();

        if(first) {
          maxSalary = value.get();
          minSalary = value.get();
        }

        if(value.get() < minSalary) { 
          minSalary = value.get();
        }

        if(value.get() > maxSalary) { 
          maxSalary = value.get();
        }

        sumSalary += value.get();
        sumSalaryPow = Math.pow(value.get(), 2);
        nbSalarie++;
      }

      avgSalary = sumSalary / nbSalarie;

      //formule  (1/n)*sum*avgSalary**2 

      standardDeviation = (1.0/nbSalarie) * sumSalaryPow * Math.pow(avgSalary,2.0);

      resultat.append("Nombre personnes: ");
      resultat.append(nbSalarie);
      resultat.append(", Salaire minimal: ");
      resultat.append(minSalary);
      resultat.append(", Salaire maximal: ");
      resultat.append(maxSalary);
      resultat.append(" Salaire moyen: ");
      resultat.append(avgSalary);
      resultat.append(", difference: ");
      resultat.append(standardDeviation);

      context.write(key, new Text(resultat.toString()));
    }
  }

  //methode main 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "bank");
    job.setJarByClass(Stats.class);
    job.setMapperClass(AgeMapper.class);
    job.setReducerClass(StatsReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
