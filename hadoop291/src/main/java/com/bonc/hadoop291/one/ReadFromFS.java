package com.bonc.hadoop291.one;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * created by G.Goe on 2018/6/29
 */
public class ReadFromFS {

    public static void main(String[] args) throws IOException {

        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

    }
}
