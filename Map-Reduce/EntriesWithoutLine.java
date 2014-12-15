import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.URL;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
       
public class EntriesWithoutLine {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      /* Data fields: ID,NomePonto,Linha,Onibus,Hora,DiaMes,Mes,Ano,Velocidade,LatitudePonto,LongitudePonto,TimeStamp,DescricaoPonto */
      String line = value.toString();
      List<String> fields = Arrays.asList(line.split(","));
      String busLine = fields.get(2);
      System.out.println(busLine);

      if(busLine.length() <= 1) {
        word.set(line);
        context.write(word, new Text(""));
      }
    }
 }
 
       
 public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {

      String txt = "";
      for(Text val: values) {
         txt += val;
         txt += " ";
      }
      context.write(key, new Text(txt));
    }
 }
    
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "stopscount");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(EntriesWithoutLine.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
