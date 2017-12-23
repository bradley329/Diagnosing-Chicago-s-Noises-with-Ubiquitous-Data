import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class ComplaintsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            int maxContentLeng = 0;
            int minContentLeng = Integer.MAX_VALUE;
            int minDateLeng = Integer.MAX_VALUE;
            int maxDateLeng = 0;
            double maxlat = 0.00000;
            double minlat = 180.00000;
            double maxlong = -180.00000;
            double minlong = -0.00000;
        for (Text value : values) {
            String ln = value.toString();
            String[] input = ln.split("\t");
            double latti = Double.parseDouble(input[0]);
            double longi = Double.parseDouble(input[1]);
            String content = input[2];
            String date = input[3];
            int contentLeng = content.length();
            int dateLeng = content.length();
            if (maxlat < latti) {
                maxlat = latti;
            }
            if (minlat > latti) {
                minlat = latti;
            }
            if (maxlong < longi) {
                maxlong = longi;
            }
            if (minlong > longi) {
                minlong = longi;
            }
            if (maxContentLeng < contentLeng) {
                maxContentLeng = contentLeng;
            }
            if (minContentLeng > contentLeng) {
                minContentLeng = contentLeng;
            }
            if (maxDateLeng < dateLeng) {
                maxDateLeng = dateLeng;
            }
            if (minDateLeng > dateLeng) {
                minDateLeng = dateLeng;
            }
        }
        String res = " Maximum of Latitude = " + Double.toString(maxlat)
                + " Minimum of Latitude = " + Double.toString(minlat)
                + " Maximum of Longitude = " + Double.toString(maxlong)
                + " Minimum of Longitude = " + Double.toString(minlong)
                + " Maximum length of Content = " + Integer.toString(maxContentLeng)
                + " Minimum length of Content = " + Integer.toString(minContentLeng)
                + " Maximum length of Date = " + Integer.toString(maxDateLeng)
                + " Minimum length of Date = " + Integer.toString(minDateLeng);

        String k = "Result:";
        context.write(new Text(k), new Text(res));
    }
}

