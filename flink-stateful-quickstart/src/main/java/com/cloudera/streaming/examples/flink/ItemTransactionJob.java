package com.cloudera.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.operators.MaxWatermark;
import com.cloudera.streaming.examples.flink.operators.SummaryAlertingCondition;
import com.cloudera.streaming.examples.flink.operators.TransactionProcessor;
import com.cloudera.streaming.examples.flink.operators.TransactionSummaryAggregator;
import com.cloudera.streaming.examples.flink.types.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.TimeUnit;

public abstract class ItemTransactionJob {

    public static final String EVENT_TIME_KEY = "event.time";

    public static OutputTag<QueryResult> QUERY_RESULT = new OutputTag<QueryResult>("query-result", TypeInformation.of(QueryResult.class));

    public final StreamExecutionEnvironment createApplicationPipeline(ParameterTool params) throws Exception {
        StreamExecutionEnvironment env = createExecutionEnvironment(params);

        DataStream<ItemTransaction> transactionStream = readTransactionStream(params, env);
        DataStream<Query> queryStream = readQueryStream(params, env)
                .assignTimestampsAndWatermarks(new MaxWatermark<>())
                .name("MaxWatermark");

        SingleOutputStreamOperator<TransactionResult> processedTransactions = transactionStream.keyBy("itemId")
                .connect(queryStream.keyBy("itemId"))
                .process(new TransactionProcessor())
                .name("Transaction Processor")
                .uid("Transaction Processor");

        DataStream<TransactionSummary> transactionSummaryStream = processedTransactions
                .keyBy("transaction.itemId")
                .timeWindow(Time.minutes(1))
                .aggregate(new TransactionSummaryAggregator())
                .name("Create Transaction Summary")
                .uid("Create Transaction Summary")
                .filter(new SummaryAlertingCondition(params))
                .name("Filter High failure rate");

        DataStream<QueryResult> queryResultStream = processedTransactions.getSideOutput(QUERY_RESULT);

        writeTransactionResults(params, processedTransactions);
        writeQueryOutput(params, queryResultStream);
        writeTransactionSummaries(params, transactionSummaryStream);

        return env;
    }

    private StreamExecutionEnvironment createExecutionEnvironment(ParameterTool params) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setMaxParallelism(128);

        long cpInterval = params.getLong("checkpoint.interval.millis", TimeUnit.MINUTES.toMillis(1));
        if (cpInterval > 0) {
            CheckpointConfig checkpointConf = env.getCheckpointConfig();
            checkpointConf.setCheckpointInterval(cpInterval);
            checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            checkpointConf.setCheckpointTimeout(TimeUnit.HOURS.toMillis(1));
            checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            env.getConfig().setUseSnapshotCompression(true);
        }

        if (params.getBoolean(EVENT_TIME_KEY, false)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        }

        return env;
    }

    protected abstract DataStream<Query> readQueryStream(ParameterTool params, StreamExecutionEnvironment env);

    protected abstract DataStream<ItemTransaction> readTransactionStream(ParameterTool params, StreamExecutionEnvironment env);

    protected abstract void writeQueryOutput(ParameterTool params, DataStream<QueryResult> queryResultStream);

    protected abstract void writeTransactionResults(ParameterTool params, DataStream<TransactionResult> transactionResults);

    protected abstract void writeTransactionSummaries(ParameterTool params, DataStream<TransactionSummary> transactionSummaryStream);

}
