/**
 * CountWords - Count the number of words in each file in the input directory
 * 
 * @author Sudar Muthu <http://sudarmuthu.com>
 * @license Beerware ;) 
 */
package com.sudarmuthu.hadoop.countwords;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper Class
 * 
 */
public class CountWordsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line);

        while (itr.hasMoreTokens()) {
            context.write(new Text(itr.nextToken()), new IntWritable(1));
        }
    }
}
