package com.rogerguo.test.benchmark.benchmark;

import com.rogerguo.test.benchmark.hbase.HBaseSpatioTemporalStorageDriver;
import com.rogerguo.test.benchmark.predicate.QueryGenerator;
import com.rogerguo.test.benchmark.predicate.StatisticUtil;
import com.rogerguo.test.index.predicate.SpatialTemporalRangeQueryPredicate;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.rogerguo.test.benchmark.StatusRecorder;
/**
 * @author yangguo
 * @create 2022-10-05 2:39 PM
 **/
public class HBaseSpatioTemporalQueryBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        //@Param({"24h"})
        @Param({"1h", "6h", "24h", "7d"})
        public String timeLength;

        //@Param({"001", "01", "1"})
        @Param({"01"})
        public String spatialWidth;

        @Param({"data", "zipfian"})
        public String distribution;

        //String queryFilenamePrefix = "/home/ubuntu/dataset/query-fulldata/porto_fulldata_";
        //String queryFilenamePrefix = "/home/ubuntu/data1/porto/query/porto-zipfian-";
        //String queryFilenamePrefix = "/home/ubuntu/data1/porto/query/porto-";
        String queryFilenamePrefix = "/home/ubuntu/data1/geolife/query/Geolife-";

        List<SpatialTemporalRangeQueryPredicate> predicateList;

        List<String> queryResultList = new ArrayList<>();

        String queryFile;

        @Setup(Level.Trial)
        public void setup() {
            // set query
            String queryFilename = queryFilenamePrefix + distribution + "-" + timeLength + "-" + spatialWidth + ".query";
            //String queryFilename = queryFilenamePrefix + timeLength + "-" + spatialWidth + ".query";
            queryFile = queryFilename;
            predicateList = QueryGenerator.getSpatialTemporalRangeQueriesFromQueryFile(queryFilename);
        }

        @TearDown(Level.Trial)
        public void saveLog() {
            for (String log : queryResultList) {
                StatusRecorder.recordStatus("hbase-spatiotemporal-fixspatial-v3-geolife-detail.log", log);
            }
            queryResultList.clear();
            StatusRecorder.recordStatus("hbase-spatiotemporal-fixspatial-v3-geolife-detail.log", "\n\n\n");
        }
    }

    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(500)    // the number of queries
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void spatialTemporalRangeQuery1xV3(Blackhole blackhole, BenchmarkState state) {
        //String queryTable = "porto-1x-spatiotemporal-table-v3";
        String queryTable = "geolife-1x-spatiotemporal-table-v3";
        String signature = "query table: " + queryTable +  ", query file: " + state.queryFile + "\n";
        
        List<Integer> resultCountList = new ArrayList<>();
        for (SpatialTemporalRangeQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = HBaseSpatioTemporalStorageDriver.spatioTemporalRangeQuery(predicate, queryTable);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(resultCount);
            String log = String.format("time:%d, count:%d", (stop -start), resultCount);
            state.queryResultList.add(log);
        }
        System.out.println("average result count: " + StatisticUtil.calculateAverage(resultCountList));
        state.queryResultList.add(signature);
        state.queryResultList.add("total:, average count:" + StatisticUtil.calculateAverage(resultCountList) + "\n\n\n");
        
    }


    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(500)    // the number of queries
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void spatialTemporalRangeQuery2xV3(Blackhole blackhole, BenchmarkState state) {
        //String queryTable = "porto-2x-spatiotemporal-table-v3";
        String queryTable = "geolife-2x-spatiotemporal-table-v3";
        String signature = "query table: " + queryTable +  ", query file: " + state.queryFile + "\n\n";
        state.queryResultList.add(signature);
        List<Integer> resultCountList = new ArrayList<>();
        for (SpatialTemporalRangeQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = HBaseSpatioTemporalStorageDriver.spatioTemporalRangeQuery(predicate, queryTable);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(resultCount);
            String log = String.format("time:%d, count:%d", (stop -start), resultCount);
            state.queryResultList.add(log);
        }
        System.out.println("average result count: " + StatisticUtil.calculateAverage(resultCountList));
        state.queryResultList.add("total:, average count:" + StatisticUtil.calculateAverage(resultCountList) + "\n\n\n");
    }

    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(500)    // the number of queries
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void spatialTemporalRangeQuery4xV3(Blackhole blackhole, BenchmarkState state) {
        //String queryTable = "porto-4x-spatiotemporal-table-v3";
        String queryTable = "geolife-4x-spatiotemporal-table-v3";
        String signature = "query table: " + queryTable +  ", query file: " + state.queryFile + "\n\n";
        state.queryResultList.add(signature);
        List<Integer> resultCountList = new ArrayList<>();
        for (SpatialTemporalRangeQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = HBaseSpatioTemporalStorageDriver.spatioTemporalRangeQuery(predicate, queryTable);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(resultCount);
            String log = String.format("time:%d, count:%d", (stop -start), resultCount);
            state.queryResultList.add(log);
        }
        System.out.println("average result count: " + StatisticUtil.calculateAverage(resultCountList));
        state.queryResultList.add("total:, average count:" + StatisticUtil.calculateAverage(resultCountList) + "\n\n\n");
    }

    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(500)    // the number of queries
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void spatialTemporalRangeQuery8xV3(Blackhole blackhole, BenchmarkState state) {
        //String queryTable = "porto-8x-spatiotemporal-table-v3";
        String queryTable = "geolife-8x-spatiotemporal-table-v3";
        String signature = "query table: " + queryTable +  ", query file: " + state.queryFile + "\n\n";
        state.queryResultList.add(signature);
        List<Integer> resultCountList = new ArrayList<>();
        for (SpatialTemporalRangeQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = HBaseSpatioTemporalStorageDriver.spatioTemporalRangeQuery(predicate, queryTable);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(resultCount);
            String log = String.format("time:%d, count:%d", (stop -start), resultCount);
            state.queryResultList.add(log);
        }
        System.out.println("average result count: " + StatisticUtil.calculateAverage(resultCountList));
        state.queryResultList.add("total:, average count:" + StatisticUtil.calculateAverage(resultCountList) + "\n\n\n");
    }


    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(500)    // the number of queries
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void spatialTemporalRangeQuery16xV3(Blackhole blackhole, BenchmarkState state) {
        //String queryTable = "porto-16x-spatiotemporal-table-v3";
        String queryTable = "geolife-16x-spatiotemporal-table-v3";
        String signature = "query table: " + queryTable +  ", query file: " + state.queryFile + "\n\n";
        state.queryResultList.add(signature);
        List<Integer> resultCountList = new ArrayList<>();
        for (SpatialTemporalRangeQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = HBaseSpatioTemporalStorageDriver.spatioTemporalRangeQuery(predicate, queryTable);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(resultCount);
            String log = String.format("time:%d, count:%d", (stop -start), resultCount);
            state.queryResultList.add(log);
        }
        System.out.println("average result count: " + StatisticUtil.calculateAverage(resultCountList));
        state.queryResultList.add("total:, average count:" + StatisticUtil.calculateAverage(resultCountList) + "\n\n\n");
    }

    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(500)    // the number of queries
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void spatialTemporalRangeQuery32xV3(Blackhole blackhole, BenchmarkState state) {
        //String queryTable = "porto-32x-spatiotemporal-table-v3";
        String queryTable = "geolife-32x-spatiotemporal-table-v3";
        String signature = "query table: " + queryTable +  ", query file: " + state.queryFile + "\n\n";
        state.queryResultList.add(signature);
        List<Integer> resultCountList = new ArrayList<>();
        for (SpatialTemporalRangeQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = HBaseSpatioTemporalStorageDriver.spatioTemporalRangeQuery(predicate, queryTable);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(resultCount);
            String log = String.format("time:%d, count:%d", (stop -start), resultCount);
            state.queryResultList.add(log);
        }
        System.out.println("average result count: " + StatisticUtil.calculateAverage(resultCountList));
        state.queryResultList.add("total:, average count:" + StatisticUtil.calculateAverage(resultCountList) + "\n\n\n");
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(HBaseSpatioTemporalQueryBenchmark.class.getSimpleName())
                .output("hbase-spatiotemporal-fixspatial-v3-geolife.log")
                .build();

        new Runner(opt).run();
    }
}
