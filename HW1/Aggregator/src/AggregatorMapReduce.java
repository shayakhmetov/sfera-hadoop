import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;


public class AggregatorMapReduce {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.charAt(0) != '\"') {
                String[] splitted_value = line.split(",");
                context.write(new Text(splitted_value[1]), new Text(splitted_value[4]));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private HashMap<String, Integer> countries = new HashMap<String, Integer>();
        private ArrayList<Integer> values;
        public void reduce(Text key, Iterable<Text> iterable, Context context) throws IOException, InterruptedException{
            Iterator<Text> iterator = iterable.iterator();
            for(Text value: iterable){
                String country = value.toString();
                if(countries.get(country) != null){
                    countries.put(country, countries.get(country) + 1);
                }
                else{
                    countries.put(country, 1);
                }
            }
            values = new ArrayList<Integer>(countries.values());
            Collections.sort(values);
            String output_string = values.size() + "\t" +
                    get_minimum(values) + "\t" + get_median(values) +
                    "\t" + get_maximum(values) + "\t" + get_average(values)
                    + "\t" + get_dispersion(values);
            context.write(key, new Text(output_string));

        }

        private Integer get_minimum(ArrayList<Integer> values){
            return values.get(0);
        }

        private Double get_median(ArrayList<Integer> values){
            if(values.size() % 2 != 0){
                return (double) values.get(values.size()/2);
            }
            else{
                return (double)(values.get(values.size()/2) + values.get(values.size()/2 - 1)) / 2.0;
            }
        }

        private Integer get_maximum(ArrayList<Integer> values){
            return values.get(values.size() - 1);
        }

        private Double get_average(ArrayList<Integer> values){
            double sum = 0;
            for(Integer value: values){
                sum += value;
            }
            return sum / values.size();
        }

        private Double get_dispersion(ArrayList<Integer> values){
            double average = get_average(values);
            double sum = 0;
            for(Integer value: values){
                sum += Math.pow(value - average, 2);
            }
            return Math.sqrt(sum / values.size());
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "aggregation");
        job.setJarByClass(AggregatorMapReduce.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
