package com.rogerguo.test.benchmark.benchmark;

import com.rogerguo.test.benchmark.hbase.HBaseIdTemporalStorageDriver;
import com.rogerguo.test.benchmark.predicate.QueryGenerator;
import com.rogerguo.test.benchmark.predicate.StatisticUtil;
import com.rogerguo.test.index.predicate.IdTemporalQueryPredicate;
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
 * @create 2022-10-05 2:10 PM
 **/
public class HBaseIdTemporalQueryBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"1h", "6h", "24h", "7d"})
        //@Param({"1h"})
        public String timeLength;

        @Param({"data", "zipfian"})
        public String distribution;

        //String queryFilenamePrefix = "/home/ubuntu/dataset/query-fulldata/porto_fulldata_id_";
        String queryFilenamePrefix = "/home/ubuntu/data1/porto/query/porto-";
        //String queryFilenamePrefix = "/home/ubuntu/data1/geolife/query/Geolife-";

        List<IdTemporalQueryPredicate> predicateList;

        List<String> queryResultList = new ArrayList<>();

        String queryFile;

        @Setup(Level.Trial)
        public void setup() {
            // set query
            //String queryFilename = queryFilenamePrefix + timeLength + ".query";
            String queryFilename = queryFilenamePrefix + distribution + "-id-" + timeLength + ".query";
            queryFile = queryFilename;
            predicateList = QueryGenerator.getIdTemporalQueriesFromQueryFile(queryFilename);
        }

        @TearDown(Level.Trial)
        public void saveLog() {
            for (String log : queryResultList) {
                StatusRecorder.recordStatus("hbase-idtemporal-v3-deserilization-detail.log", log);
            }
            queryResultList.clear();
            StatusRecorder.recordStatus("hbase-idtemporal-v3-deserilization-detail.log", "\n\n\n");
        }

    }

    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(500)    // the number of queries
    @Measurement(time = 5, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void idTemporalQuery1xV3(Blackhole blackhole, BenchmarkState state) {
        String queryTable = "porto-1x-id-temporal-table-v3";
        //String queryTable = "geolife-1x-id-temporal-table-v3";
        String signature = "query table: " + queryTable + ", query file: " + state.queryFile + "\n";

        List<Integer> resultCountList = new ArrayList<>();
        for (IdTemporalQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = HBaseIdTemporalStorageDriver.idTemporalQuery(predicate, queryTable);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(predicate);
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
    public void idTemporalQuery2xV3(Blackhole blackhole, BenchmarkState state) {
        String queryTable = "porto-2x-id-temporal-table-v3";
        //String queryTable = "geolife-2x-id-temporal-table-v3";
        String signature = "query table: " + queryTable + ", query file: " + state.queryFile + "\n";

        List<Integer> resultCountList = new ArrayList<>();
        for (IdTemporalQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = HBaseIdTemporalStorageDriver.idTemporalQuery(predicate, queryTable);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(predicate);
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
    public void idTemporalQuery4xV3(Blackhole blackhole, BenchmarkState state) {
        String queryTable = "porto-4x-id-temporal-table-v3";
        //String queryTable = "geolife-4x-id-temporal-table-v3";
        String signature = "query table: " + queryTable + ", query file: " + state.queryFile + "\n";

        List<Integer> resultCountList = new ArrayList<>();
        for (IdTemporalQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = HBaseIdTemporalStorageDriver.idTemporalQuery(predicate, queryTable);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(predicate);
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
    public void idTemporalQuery8xV3(Blackhole blackhole, BenchmarkState state) {
        String queryTable = "porto-8x-id-temporal-table-v3";
        //String queryTable = "geolife-8x-id-temporal-table-v3";
        String signature = "query table: " + queryTable + ", query file: " + state.queryFile + "\n";

        List<Integer> resultCountList = new ArrayList<>();
        for (IdTemporalQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = HBaseIdTemporalStorageDriver.idTemporalQuery(predicate, queryTable);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(predicate);
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
    public void idTemporalQuery16xV3(Blackhole blackhole, BenchmarkState state) {
        String queryTable = "porto-16x-id-temporal-table-v3";
        //String queryTable = "geolife-16x-id-temporal-table-v3";
        String signature = "query table: " + queryTable + ", query file: " + state.queryFile + "\n";

        List<Integer> resultCountList = new ArrayList<>();
        for (IdTemporalQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = HBaseIdTemporalStorageDriver.idTemporalQuery(predicate, queryTable);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(predicate);
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
    public void idTemporalQuery32xV3(Blackhole blackhole, BenchmarkState state) {
        String queryTable = "porto-32x-id-temporal-table-v3";
        //String queryTable = "geolife-32x-id-temporal-table-v3";
        String signature = "query table: " + queryTable + ", query file: " + state.queryFile + "\n";

        List<Integer> resultCountList = new ArrayList<>();
        for (IdTemporalQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = HBaseIdTemporalStorageDriver.idTemporalQuery(predicate, queryTable);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(predicate);
            String log = String.format("time:%d, count:%d", (stop -start), resultCount);
            state.queryResultList.add(log);
        }
        System.out.println("average result count: " + StatisticUtil.calculateAverage(resultCountList));
        state.queryResultList.add(signature);
        state.queryResultList.add("total:, average count:" + StatisticUtil.calculateAverage(resultCountList) + "\n\n\n");
    }

    public static void main(String[] args) throws RunnerException {
        System.out.println("This is id temporal query benchmark for hbase");

        Options opt = new OptionsBuilder()
                .include(HBaseIdTemporalQueryBenchmark.class.getSimpleName())
                .output("hbase-idtemporal-v3-deserilization.log")
                .build();

        new Runner(opt).run();
    }
}
