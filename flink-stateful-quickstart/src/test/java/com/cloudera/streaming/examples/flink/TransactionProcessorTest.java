package com.cloudera.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.types.*;
import com.cloudera.streaming.examples.flink.utils.testing.CollectingSink;
import com.cloudera.streaming.examples.flink.utils.testing.JobTester;
import com.cloudera.streaming.examples.flink.utils.testing.ManualSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransactionProcessorTest extends ItemTransactionJob {

    private ManualSource<ItemTransaction> transactionSource;
    private ManualSource<Query> querySource;

    private CollectingSink<QueryResult> queryResultSink = new CollectingSink<>();
    private CollectingSink<TransactionResult> transactionResultSink = new CollectingSink<>();

    @Test
    public void runTest() throws Exception {
        JobTester.startTest(createApplicationPipeline(ParameterTool.fromArgs(new String[]{})));

        ItemTransaction it1 = new ItemTransaction(1, 2, "item_1", 100);
        transactionSource.sendRecord(it1);
        assertEquals(new TransactionResult(it1, true), transactionResultSink.poll());

        querySource.sendRecord(new Query(0, "item_1"));
        assertEquals(new QueryResult(0, new ItemInfo("item_1", 100)), queryResultSink.poll());

        querySource.sendRecord(new Query(3, "item_2"));
        assertEquals(new QueryResult(3, null), queryResultSink.poll());

        JobTester.stopTest();

        assertTrue(transactionResultSink.isEmpty());
        assertTrue(queryResultSink.isEmpty());
    }

    @Override
    public void writeQueryOutput(ParameterTool params, DataStream<QueryResult> queryResultStream) {
        queryResultStream.addSink(queryResultSink);
    }

    @Override
    protected void writeTransactionResults(ParameterTool params, DataStream<TransactionResult> transactionResults) {
        transactionResults.addSink(transactionResultSink);
    }

    @Override
    protected void writeTransactionSummaries(ParameterTool params, DataStream<TransactionSummary> transactionSummaryStream) {
        //ignore
    }

    @Override
    public DataStream<Query> readQueryStream(ParameterTool params, StreamExecutionEnvironment env) {
        querySource = JobTester.createManualSource(env, TypeInformation.of(Query.class));
        return querySource.getDataStream();
    }

    @Override
    public DataStream<ItemTransaction> readTransactionStream(ParameterTool params, StreamExecutionEnvironment env) {
        transactionSource = JobTester.createManualSource(env, TypeInformation.of(ItemTransaction.class));
        return transactionSource.getDataStream();
    }
}
