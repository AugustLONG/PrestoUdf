/**
 * Created by sylar on 04/01/2017.
 */

import org.rocksdb.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RockDBTest {
    static {
        RocksDB.loadLibrary();
    }

    public static void main(String args[]) throws RocksDBException, IOException {

        String dbfile = "/tmp/test";
        Options options = new Options().setCreateIfMissing(true);
//        options.allowConcurrentMemtableWrite();
        options.allowMmapWrites();
//        options.getEnv().setBackgroundThreads(100);
//        options.setMaxBackgroundCompactions(2);
//        options.setAllowOsBuffer(true);
//        options.setWriteBufferSize(500 * 1024 * 1024*1024);
//        options.setMaxWriteBufferNumber(5);
//        options.setMinWriteBufferNumberToMerge(2);


        RocksDB db = null;
        try {
            db = RocksDB.open(options, dbfile);
        } catch (RocksDBException e) {
        }
        long st = System.currentTimeMillis();

        FileReader reader = new FileReader("/Users/sylar/Desktop/bfa_2016-12-01.txt");
        BufferedReader br = new BufferedReader(reader);
        String str = null;
        while ((str = br.readLine()) != null) {
            db.put(str.getBytes(), "".getBytes());
        }
        System.out.println(System.currentTimeMillis() - st);

        br.close();


        if (db != null) db.close();


//        ReadOptions readOptions = new ReadOptions();
//        readOptions.setPrefixSameAsStart(true);
//        RocksIterator iterator = db.newIterator(readOptions);
//
//
//        for (iterator.seek("x".getBytes()); iterator.isValid(); iterator
//                .next()) {
//            String key = new String(iterator.key());
//            if (!key.startsWith("x")) {
//                break;
//            }
//
//            System.out.println(String.format("%s:%s", key, new String(db.get(key.getBytes()))));
//        }

    }
}
