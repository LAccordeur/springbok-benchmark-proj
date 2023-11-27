package com.rogerguo.test.benchmark.benchmark;

import com.rogerguo.test.benchmark.predicate.QueryGenerator;
import com.rogerguo.test.benchmark.predicate.StatisticUtil;
import com.rogerguo.test.benchmark.springbok.SpringbokDriver;
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
 * @create 2022-06-28 2:08 PM
 **/
public class SpringbokIdTemporalBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        @Param({"1h", "6h", "24h", "7d"})
        //@Param({"7d"})
        public String timeLength;

        //@Param({"data", "zipfian"})
        @Param({"data"})
        public String distribution;

        //String queryFilenamePrefix = "/home/ubuntu/data1/porto/query/porto-data-id-";

        //String queryFilenamePrefix = "/home/ubuntu/data1/geolife/query/Geolife-";
        String queryFilenamePrefix = "/home/ubuntu/data1/porto/query/porto-";

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
                StatusRecorder.recordStatus("test-springbok-id-temporal-chunksize-100-detail.log", log);
            }
            queryResultList.clear();
            StatusRecorder.recordStatus("test-springbok-id-temporal-chunksize-100-detail.log", "\n\n\n");
        }

    }

    @Fork(value = 1)
    @Warmup(iterations = 1, time = 5)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OperationsPerInvocation(500)    // the number of queries
    @Measurement(time = 1, iterations = 3)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void idTemporalQuery(Blackhole blackhole, BenchmarkState state) {
        String queryTable = "porto-1x-table-v3";
        String signature = "query table: " + queryTable +  ", query file: " + state.queryFile + "\n";


        List<Integer> resultCountList = new ArrayList<>();
        for (IdTemporalQueryPredicate predicate : state.predicateList) {
            System.out.println(predicate);
            long start = System.currentTimeMillis();
            int resultCount = SpringbokDriver.idTemporalQuery(predicate);
            long stop = System.currentTimeMillis();
            System.out.println("result count: " + resultCount);
            resultCountList.add(resultCount);
            blackhole.consume(predicate);
            String log = String.format("time:%d, count:%d", (stop -start), resultCount);
            state.queryResultList.add(log);
        }

        System.out.println("average result count: " + StatisticUtil.calculateAverage(resultCountList));
        state.queryResultList.add(signature);
        state.queryResultList.add("total:, average count:" + StatisticUtil.calculateAverage(resultCountList));
    }

    public static void main(String[] args) throws RunnerException {
        System.setProperty("sun.net.httpserver.nodelay", "true");
        Options opt = new OptionsBuilder()
                .include(SpringbokIdTemporalBenchmark.class.getSimpleName())
                .output("test-springbok-id-temporal-chunksize-100.log")
                .build();

        new Runner(opt).run();
    }

}
