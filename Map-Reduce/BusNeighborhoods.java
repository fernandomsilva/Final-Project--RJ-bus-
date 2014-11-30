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
        
public class BusNeighborhoods {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    HashMap<String, String> neighborhoodsBoundaries = new HashMap<String, String>(); 
    private static double PI = 3.14159265;
    private static double TWOPI = 2*PI;

    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
      /* neighborhood:lat0,lng0 lat1,lng1 ... latN,lngN */

      try {
        URL url = new URL("https://s3.amazonaws.com/rio-cleaned-data/boundaries.txt");
        Scanner s = new Scanner(url.openStream());
        while (s.hasNextLine()) {
          String line = s.nextLine();
          List<String> fields = Arrays.asList(line.split(":"));
          //System.out.println(fields.get(0));
          neighborhoodsBoundaries.put(fields.get(0), fields.get(1));
        }
        s.close();
      } catch (IOException e) {
	e.printStackTrace();
      }
    }
   
    //Method in http://stackoverflow.com/questions/4287780/detecting-whether-a-gps-coordinate-falls-within-a-polygon-on-a-map
    private static boolean coordinate_is_inside_polygon(double latitude, double longitude, ArrayList<Double> lat_array, ArrayList<Double> long_array) {       
       int i;
       double angle = 0;
       double point1_lat;
       double point1_long;
       double point2_lat;
       double point2_long;
       int n = lat_array.size();

       for (i = 0; i < n; i++) {
          point1_lat = lat_array.get(i) - latitude;
          point1_long = long_array.get(i) - longitude;
          point2_lat = lat_array.get((i+1)%n) - latitude; 
          point2_long = long_array.get((i+1)%n) - longitude;
          angle += Angle2D(point1_lat,point1_long,point2_lat,point2_long);
       }

       //System.out.println(Math.abs(angle));
       if (Math.abs(angle) < PI) {
          return false;
       }
       return true;
    }

    //Method in http://stackoverflow.com/questions/4287780/detecting-whether-a-gps-coordinate-falls-within-a-polygon-on-a-map
    private static double Angle2D(double y1, double x1, double y2, double x2) {
      double dtheta,theta1,theta2;

      theta1 = Math.atan2(y1,x1);
      theta2 = Math.atan2(y2,x2);
      dtheta = theta2 - theta1;
      //System.out.println(theta1 + " " + theta2 + " " + dtheta);
      while (dtheta > PI) {
        dtheta -= TWOPI;
      }

      while (dtheta < -PI) {
        dtheta += TWOPI;
      }
      return dtheta;
    }

    //Method in http://stackoverflow.com/questions/4287780/detecting-whether-a-gps-coordinate-falls-within-a-polygon-on-a-map
    private static boolean is_valid_gps_coordinate(double latitude, double longitude) {
      //This is a bonus function, it's unused, to reject invalid lat/longs.
      if (latitude > -90 && latitude < 90 && longitude > -180 && longitude < 180) {
        return true;
      }
      return false;
    } 

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /* Data fields: ID,NomePonto,Linha,Onibus,Hora,DiaMes,Mes,Ano,Velocidade,LatitudePonto,LongitudePonto,TimeStamp,DescricaoPonto */
        String line = value.toString();
        List<String> fields = Arrays.asList(line.split(","));
        String id = fields.get(0);
        double lat = Double.parseDouble(fields.get(9));
        double lng = Double.parseDouble(fields.get(10));
        String busLine = fields.get(2); 

        String neighborhood = "--";
        for(java.util.Map.Entry<String, String> entry: neighborhoodsBoundaries.entrySet()) {
          neighborhood = entry.getKey();
          List<String> boundaries = Arrays.asList(entry.getValue().split(" "));
          ArrayList<Double> lat_array = new ArrayList<Double>();
          ArrayList<Double> lng_array = new ArrayList<Double>(); 
          for(String s: boundaries) {
            List<String> coords = Arrays.asList(s.split(","));
            if(coords.size() == 2 && coords.get(0).matches("-?\\d+(\\.\\d+)?") && coords.get(1).matches("-?\\d+(\\.\\d+)?")) {
              double coordLat = Double.parseDouble(coords.get(0));
              double coordLng = Double.parseDouble(coords.get(1));
              //System.out.println(lat + " " + lng + " " + coordLat + " " + coordLng);
                lat_array.add(Double.parseDouble(coords.get(1)));
                lng_array.add(Double.parseDouble(coords.get(0)));
            }
          }
          boolean inside = coordinate_is_inside_polygon(lat,lng, lat_array, lng_array);
          if(inside) {
            //System.out.println("Bus " + id + " is inside " + neighborhood);
            break; 
          } 
        }
 
        word.set(busLine); //aggregating neighborhoods by bus lines
        context.write(word, new Text(neighborhood));
    }
 } 
       
 public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {

      String txt = "";
      for(Text val: values) {
         txt += val;
         txt += ", ";
      }
      
      context.write(key, new Text(txt));
    }
 }
    
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "averagedistance");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(BusNeighborhoods.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
