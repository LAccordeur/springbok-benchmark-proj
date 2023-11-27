package com.rogerguo.test.benchmark.benchmark;

import com.rogerguo.test.benchmark.PortoTaxiRealData;
import com.rogerguo.test.benchmark.springbok.SpringbokDriver;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.benchmark.DataProducer;
import com.rogerguo.test.benchmark.GeolifeRealData;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yangguo
 * @create 2022-06-28 2:39 PM
 **/
public class SpringbokQueryTableCreator {

    public static void createAndInsertData(String datasetName) {
        //String dataFile = "/home/ubuntu/data/porto_data_v1_5x.csv";

        //String dataFile = "/home/ubuntu/data1/porto/dataset/porto_data_v3.csv";
        //String dataFile = "/home/ubuntu/data1/porto/dataset/porto_data_v1_15x.csv";
        //PortoTaxiRealData portoTaxiRealData = new PortoTaxiRealData(dataFile);

        DataProducer dataProducer;
        if ("porto".equals(datasetName)) {
            dataProducer = new PortoTaxiRealData("/home/ubuntu/data1/porto/dataset/porto_data_v3_32x.csv");
        } else if ("geolife".equals(datasetName)) {
            dataProducer = new GeolifeRealData("/home/ubuntu/data1/geolife/dataset/Geolife_v3.csv");
        } else {
            System.out.println("no supported dataset");
            return;
        }


        List<TrajectoryPoint> dataBatch = new ArrayList<>();
        TrajectoryPoint point;
        long count = 0;
        while ((point = dataProducer.nextPoint()) != null) {
            count++;
            dataBatch.add(point);
            if (dataBatch.size() == 20000) {
                SpringbokDriver.insertData(dataBatch);
                dataBatch.clear();
            }
            if (count % 1000000 == 0) {
                System.out.println(point);
                System.out.println("count: " + count);
                
            }
            
        }

    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        createAndInsertData("porto");
        long stop = System.currentTimeMillis();
        System.out.println("insertion time: " + (stop - start) + " ms");
    }

}
