/**
 * CountWords - Count the number of words in each file in the input directory
 *  
 * @author Sudar Muthu <http://sudarmuthu.com>
 * @license Beerware ;)
 */
package com.sudarmuthu.hadoop.countwords;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class
 * 
 */
public class CountWordsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
