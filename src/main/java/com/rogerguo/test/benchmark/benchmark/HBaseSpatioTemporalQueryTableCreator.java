package com.rogerguo.test.benchmark.benchmark;

import com.rogerguo.test.benchmark.PortoTaxiRealData;
import com.rogerguo.test.benchmark.DataProducer;
import com.rogerguo.test.benchmark.GeolifeRealData;
import com.rogerguo.test.benchmark.geomesa.client.TimeRecorder;
import com.rogerguo.test.benchmark.hbase.HBaseSpatioTemporalStorageDriver;
import com.rogerguo.test.common.TrajectoryPoint;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangguo
 * @create 2022-10-05 2:34 PM
 **/
public class HBaseSpatioTemporalQueryTableCreator {

    //public static String FILENAME = "/home/ubuntu/data/porto_data_v1_5x.csv";

    public static String TABLE_NAME = "porto-5x-spatiotemporal-table";

    public static int createTableAndInsert(String tableName, String dataFilename, String datasetName) {

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
        HBaseSpatioTemporalStorageDriver.createTable(tableName);
        List<TrajectoryPoint> dataBatch = new ArrayList<>();

        TrajectoryPoint point;
        int count = 0;
        while ((point = dataProducer.nextPoint()) != null) {
            count++;
            dataBatch.add(point);
            //System.out.println(point.getTimestamp() + ", " + point.getOid());
            if (dataBatch.size() == 20000) {
                HBaseSpatioTemporalStorageDriver.batchPutTrajectoryPoints(dataBatch, tableName);
                dataBatch.clear();
            }
            if (count % 1000000 == 0) {
                System.out.println(point);
                System.out.println("count: " + count);
            }

        }
        HBaseSpatioTemporalStorageDriver.batchPutTrajectoryPoints(dataBatch, tableName);
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
        // String dataFilename = "/home/ubuntu/data1/porto/dataset/porto_data_v3.csv";
        // String tableName = "porto-spatiotemporal-table";
        // String datasetType = "porto";
        String basicInfo = String.format("[%s, %s, %s]\n", dataFilename, tableName, datasetType);
        System.out.println(basicInfo);
        TimeRecorder.recordTime(logFilename, basicInfo);

        long start = System.currentTimeMillis();
        int count = createTableAndInsert(tableName, dataFilename, datasetType);
        long stop = System.currentTimeMillis();
        System.out.println("total record count: " + count);
        System.out.println("insertion time: " + (stop - start) + " ms");
        String log = String.format("[HBase][Spatio Temporal Insertion] record count: %d, total time: %d ms \n\n", count, (stop - start));
        TimeRecorder.recordTime(logFilename, log);

    

        // long start = System.currentTimeMillis();
        // int count = createTableAndInsert();
        // long stop = System.currentTimeMillis();
        // System.out.println("total record count: " + count);
        // System.out.println("insertion time: " + (stop - start) + " ms");
        // String log = String.format("[HBase][Spatio Temporal Insertion] record count: %d, total time: %d ms \n", count, (stop - start));
        // TimeRecorder.recordTime("insertion-time.log", log);
    }
}
