package com.rogerguo.test.benchmark.benchmark;

import com.rogerguo.test.benchmark.PortoTaxiRealData;
import com.rogerguo.test.benchmark.geomesa.client.TimeRecorder;
import com.rogerguo.test.benchmark.hbase.HBaseIdTemporalStorageDriver;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.benchmark.DataProducer;
import com.rogerguo.test.benchmark.GeolifeRealData;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangguo
 * @create 2022-10-05 1:55 PM
 **/
public class HBaseIdTemporalQueryTableCreator {

    //public static String FILENAME = "/home/ubuntu/data/porto_data_v1_5x.csv";

    public static String TABLE_NAME = "porto-5x-id-temporal-table";

    public static long createTableAndInsert(String tableName, String dataFilename, String datasetName) {
        
        DataProducer dataProducer;
        if ("porto".equals(datasetName)) {
            dataProducer = new PortoTaxiRealData(dataFilename);
        } else if ("geolife".equals(datasetName)) {
            dataProducer = new GeolifeRealData(dataFilename);
        } else {
            System.out.println("no supported dataset");
            return 0;
        }

        //PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData(dataFilename);
        HBaseIdTemporalStorageDriver.createTable(tableName);
        List<TrajectoryPoint> dataBatch = new ArrayList<>();

        TrajectoryPoint point;
        long count = 0;
        while ((point = dataProducer.nextPoint()) != null) {
            count++;
            dataBatch.add(point);
            //System.out.println(point.getTimestamp() + ", " + point.getOid());
            if (dataBatch.size() == 20000) {
                HBaseIdTemporalStorageDriver.batchPutTrajectoryPoints(dataBatch, tableName);
                dataBatch.clear();
            }
            if (count % 1000000 == 0) {
                System.out.println(point);
                System.out.println("count: " + count);
            }

        }
        HBaseIdTemporalStorageDriver.batchPutTrajectoryPoints(dataBatch, TABLE_NAME);
        return count;
    }

    public static void main(String[] args) {

        if (args.length != 3) {
            System.out.println("please provides 3 parameters");
        }

        String logFilename = "hbase-insertion-v3.log";

        String dataFilename = args[0];
        String tableName = args[1];
        String datasetType = args[2];
        String basicInfo = String.format("[%s, %s, %s]\n", dataFilename, tableName, datasetType);
        System.out.println(basicInfo);
        TimeRecorder.recordTime(logFilename, basicInfo);

        long start = System.currentTimeMillis();
        long count = createTableAndInsert(tableName, dataFilename, datasetType);
        long stop = System.currentTimeMillis();
        System.out.println("total record count: " + count);
        System.out.println("insertion time: " + (stop - start) + " ms");
        String log = String.format("[HBase][ID Temporal Insertion] record count: %d, total time: %d ms \n", count, (stop - start));
        TimeRecorder.recordTime(logFilename, log);
    }
}
