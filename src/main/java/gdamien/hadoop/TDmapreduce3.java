package gdamien.hadoop;

/**
 * Created by Gauthier on 08/10/2016.
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TDmapreduce3 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] tabJetons = value.toString().split(";");

            String genre;
            String[] tabGenre = tabJetons[1].split(",");

            System.out.println(tabGenre.length);
            for (int i=0;i<tabGenre.length;i++) {
                genre = tabGenre[i].trim();
                if (genre.equals("")) {
                    genre = "NR";
                }
                word.set(genre);
                context.write(word, one);
            }
        }
    }


    public static class DoubleReducer
            extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable result =new DoubleWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(TDmapreduce3.class);
        job.setMapperClass(TokenizerMapper.class);
/*      We do not include the combiner step to avoid issues with averaging */
/*        job.setCombinerClass(IntSumReducer.class); */
        job.setReducerClass(DoubleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}