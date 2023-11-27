package com.rogerguo.test.benchmark.benchmark;

import com.rogerguo.test.benchmark.springbok.SpringbokDriver;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import java.util.*;

import com.rogerguo.test.common.Point;
import com.rogerguo.test.benchmark.hbase.HBaseIdTemporalStorageDriver;
import com.rogerguo.test.benchmark.hbase.HBaseSpatioTemporalStorageDriver;

import com.rogerguo.test.benchmark.PortoTaxiRealData;
import com.rogerguo.test.benchmark.DataProducer;
import com.rogerguo.test.benchmark.GeolifeRealData;
import com.rogerguo.test.benchmark.geomesa.client.TimeRecorder;

import com.rogerguo.test.common.TrajectoryPoint;

public class SpringbokTest {

    public static void main(String[] args) {
        SpatialTemporalRangeQueryPredicate predicate = new SpatialTemporalRangeQueryPredicate(1373060632000L, 1373147032000L, new Point(-8.64153, 41.156955), new Point(-8.63153, 41.166955));
        SpatialTemporalRangeQueryPredicate predicate2 = new SpatialTemporalRangeQueryPredicate(1372679848000L, 1372766248000L, new Point(-8.691255, 41.196726), new Point(-8.681255, 41.206726));
        
        // 1316967512000,1316971112000,116.327153,116.337153,39.978582,39.988582
        SpatialTemporalRangeQueryPredicate predicate3 = new SpatialTemporalRangeQueryPredicate(1316967512000L, 1316971112000L, new Point(116.327153, 39.978582), new Point(116.337153, 39.988582));

        // 1225491737000,1226096537000,116.318357,116.328357,40.009101,40.019101
        SpatialTemporalRangeQueryPredicate predicate4 = new SpatialTemporalRangeQueryPredicate(1225491737000L,1226096537000L, new Point(116.318357, 40.009101), new Point(116.328357, 40.019101));


        IdTemporalQueryPredicate idPredicate = new IdTemporalQueryPredicate(1402685057000L, 1403289857000L, "20000345");
        IdTemporalQueryPredicate idPredicate2 = new IdTemporalQueryPredicate(1373340155000L, 1373944955000L, "20000520");
        IdTemporalQueryPredicate idPredicate3 = new IdTemporalQueryPredicate(1321744247000L, 1321747847000L, "165");

        IdTemporalQueryPredicate idPredicate4 = new IdTemporalQueryPredicate(1213663791000L,1244268591000L, "039");

        //165,1321744247000,1321747847000
        //long start = System.currentTimeMillis();
        //int resultCount = SpringbokDriver.spatialTemporalQuery(predicate);
        
        //int resultCount = HBaseSpatioTemporalStorageDriver.spatioTemporalRangeQuery(predicate, HBaseSpatioTemporalQueryTableCreator.TABLE_NAME);
        
        System.setProperty("sun.net.httpserver.nodelay", "true");
        //int resultCount = HBaseSpatioTemporalStorageDriver.spatioTemporalRangeQuery(predicate3, "geolife-1x-spatiotemporal-table-v3");
        //int resultCount = SpringbokDriver.spatialTemporalQuery(predicate4);
        int resultCount = SpringbokDriver.idTemporalQuery(idPredicate2);
        //queryRawData();  
        long start = System.currentTimeMillis(); 
        int resultCount2 = SpringbokDriver.idTemporalQuery(idPredicate2);   
        //int resultCount2 = HBaseIdTemporalStorageDriver.idTemporalQuery(idPredicate3, "geolife-32x-id-temporal-table-v3");   
        //int resultCount2 = HBaseSpatioTemporalStorageDriver.spatioTemporalRangeQuery(predicate3, "geolife-1x-spatiotemporal-table-v3");
        //int resultCount2 = SpringbokDriver.spatialTemporalQuery(predicate4);
        
        

        long stop = System.currentTimeMillis();
        System.out.println("result count: " + resultCount);
        System.out.println("time: " + (stop - start));

        //checkDataset();

        //queryRawData();
        //queryRawDataST();

        
    }

    public static void queryRawData() {

        IdTemporalQueryPredicate idPredicate = new IdTemporalQueryPredicate(1321744247000L, 1321747847000L, "165");

        DataProducer dataProducer = new GeolifeRealData("/home/ubuntu/data1/geolife/dataset/Geolife_v3_32x.csv");
        TrajectoryPoint point;
        int count = 0;
        Set<String> set = new HashSet<>();
        while ((point = dataProducer.nextPoint()) != null) {
            if (point.getOid().equals(idPredicate.getDeviceId()) && point.getTimestamp() <= idPredicate.getStopTimestamp() && point.getTimestamp() >= idPredicate.getStartTimestamp()) {
                count++;
                String rowKey = point.getOid() + "." + point.getTimestamp();
                set.add(rowKey);
            }
            
            if (point.getTimestamp() > idPredicate.getStopTimestamp()) {
                break;
            }
        }
        System.out.println("result count: " + count);
        System.out.println("unqiue count: " + set.size());
        
    }

    public static void queryRawDataST() {
        SpatialTemporalRangeQueryPredicate predicate = new SpatialTemporalRangeQueryPredicate(1316967512000L, 1316971112000L, new Point(116.327153, 39.978582), new Point(116.337153, 39.988582));

        DataProducer dataProducer = new GeolifeRealData("/home/ubuntu/data1/geolife/dataset/Geolife_v3_2x.csv");
        TrajectoryPoint point;
        int count = 0;
        Set<String> set = new HashSet<>();
        while ((point = dataProducer.nextPoint()) != null) {
            if (point.getLongitude() >= predicate.getLowerLeft().getLongitude() && point.getLongitude() <= predicate.getUpperRight().getLongitude()
                                    && point.getLatitude() >= predicate.getLowerLeft().getLatitude() && point.getLatitude() <= predicate.getUpperRight().getLatitude()
                                    && point.getTimestamp() >= predicate.getStartTimestamp() && point.getTimestamp() <= predicate.getStopTimestamp()) {
                                count++;
                                //System.out.println(item);
                                //System.out.println(dataValue);
                            }
            
            if (point.getTimestamp() > predicate.getStopTimestamp()) {
                break;
            }
        }
        System.out.println("result count: " + count);
        System.out.println("unqiue count: " + set.size());
    }

    public static void checkDataset() {
        DataProducer dataProducer = new GeolifeRealData("/home/ubuntu/data1/geolife/dataset/Geolife_v3.csv");
        TrajectoryPoint point;
        int count = 0;
        Set<String> set = new HashSet<>();
        while ((point = dataProducer.nextPoint()) != null) {
            String rowKey = point.getOid() + "." + point.getTimestamp();
            set.add(rowKey);
            count++;
        }
        System.out.println("result count: " + count);
        System.out.println("unique count: " + set.size());
    }

    
}
