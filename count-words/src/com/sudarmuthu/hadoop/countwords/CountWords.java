/**
 * CountWords - Count the number of words in each file in the input directory
 * 
 * @author Sudar Muthu <http://sudarmuthu.com>
 * @license Beerware ;)
 */
package com.sudarmuthu.hadoop.countwords;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Main class
 *
 */
public class CountWords {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
          System.err.println("Usage: CountWords <input path> <output path>");
          System.exit(-1);
        }
        
        Job job = new Job();
        job.setJarByClass(CountWords.class);
        job.setJobName("Count Words");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(CountWordsMapper.class);
        job.setReducerClass(CountWordsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}
