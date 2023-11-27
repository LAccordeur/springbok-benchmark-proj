package com.rogerguo.test.benchmark.springbok;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rogerguo.test.benchmark.predicate.QueryGenerator;
import com.rogerguo.test.client.SpringbokClient;
import com.rogerguo.test.common.TrajectoryPoint;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;

import java.util.*;
import org.xerial.snappy.Snappy;
/**
 * @author yangguo
 * @create 2022-06-28 11:35 AM
 **/
public class SpringbokDriver {

    private static SpringbokClient client = new SpringbokClient("http://172.31.85.41:8001");

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        /*List<IdTemporalQueryPredicate> predicateList = QueryGenerator.getIdTemporalQueriesFromQueryFile("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/query-on-10w/porto_10w_id_7d.query");
        long start = System.currentTimeMillis();
        for (IdTemporalQueryPredicate predicate : predicateList) {
            int count = idTemporalQuery(predicate);
            System.out.println("count: " + count);
        }
        long stop = System.currentTimeMillis();
        System.out.println("50 queries time: " + (stop - start) + " ms");*/

        /*IdTemporalQueryPredicate predicate = new IdTemporalQueryPredicate(1372638215000L,1373243015000L,"20000337");
        List<TrajectoryPoint> pointList = idTemporalQueryForCheck(predicate);
        Set<Long> hashSet = new HashSet<>();
        for (TrajectoryPoint point : pointList) {
            hashSet.add(point.getTimestamp());
        }
        System.out.println(hashSet.size());*/

        List<SpatialTemporalRangeQueryPredicate> predicateList = QueryGenerator.getSpatialTemporalRangeQueriesFromQueryFile("/home/yangguo/Data/DataSet/Trajectory/TaxiPorto/archive/query-on-10w/porto_10w_24h_01.query");
        long start = System.currentTimeMillis();
        for (SpatialTemporalRangeQueryPredicate predicate : predicateList) {
            System.out.println(predicate);
            int count = spatialTemporalQuery(predicate);
            System.out.println("count: " + count);
        }
        long stop = System.currentTimeMillis();
        System.out.println("50 queries time: " + (stop - start) + " ms");

    }

    public static void stopServer() {
        try {
            client.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insertData(List<TrajectoryPoint> trajectoryPointList) {
        try {
            client.insert(trajectoryPointList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insertDataAsync(List<TrajectoryPoint> trajectoryPointList) {
        try {
            client.insert(trajectoryPointList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int idTemporalQuery(IdTemporalQueryPredicate predicate) {
        try {
            //String result = client.idTemporalQueryWithCompressionTransfer(predicate);
            String result = client.idTemporalQuery(predicate);
            //List<TrajectoryPoint> pointList = objectMapper.readValue(result, new TypeReference<List<TrajectoryPoint>>() {});

            //return pointList.size();
            String[] records = result.split(";");
            //return pointList.size();
            return records.length;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static List<TrajectoryPoint> idTemporalQueryForCheck(IdTemporalQueryPredicate predicate) {
        try {
            //String result = client.idTemporalQueryWithCompressionTransfer(predicate);
            String result = client.idTemporalQuery(predicate);
            List<TrajectoryPoint> pointList = objectMapper.readValue(result, new TypeReference<List<TrajectoryPoint>>() {});

            pointList.sort(Comparator.comparingLong(TrajectoryPoint::getTimestamp));
            for (TrajectoryPoint point : pointList) {
                System.out.println(point);
            }
            return pointList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    static long resultSizeSum = 0;
    static int queryCount = 0;
    public static int spatialTemporalQuery(SpatialTemporalRangeQueryPredicate predicate) {
        try {
            //String result = client.spatialTemporalQueryWithCompressionTransfer(predicate);
            String result = client.spatialTemporalQuery(predicate);
            System.out.println("result size: " + result.length());
            resultSizeSum += result.length() / 1024;
            queryCount++;
            System.out.println("avg result size sum: " + (resultSizeSum / queryCount));
            //System.out.println(result);
            //List<TrajectoryPoint> pointList = objectMapper.readValue(result, new TypeReference<List<TrajectoryPoint>>() {});
            //System.out.println(result);
            String[] records = result.split(";");
            //return pointList.size();
            return records.length;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }



}
