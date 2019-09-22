package com.cloudera.streaming.examples.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.cloudera.streaming.examples.flink.operators.ItemTransactionGeneratorSource;
import com.cloudera.streaming.examples.flink.operators.QueryStringParser;
import com.cloudera.streaming.examples.flink.types.ItemTransaction;
import com.cloudera.streaming.examples.flink.types.Query;
import com.cloudera.streaming.examples.flink.types.QueryResult;
import com.cloudera.streaming.examples.flink.types.TransactionResult;
import com.cloudera.streaming.examples.flink.types.TransactionSummary;

public class SocketTransactionProcessorJob extends ItemTransactionJob {

	public static void main(String[] args) throws Exception {
		new SocketTransactionProcessorJob()
				.createApplicationPipeline(ParameterTool.fromArgs(new String[]{"--minimum.summary.vol", "850", "--sleep", "1"}))
				.execute();
	}

	@Override
	public void writeQueryOutput(ParameterTool params, DataStream<QueryResult> queryResultStream) {
		queryResultStream.printToErr();
	}

	@Override
	protected void writeTransactionResults(ParameterTool params, DataStream<TransactionResult> transactionresults) {
		// Ignore them for now
	}

	@Override
	protected void writeTransactionSummaries(ParameterTool params, DataStream<TransactionSummary> transactionSummaryStream) {
		transactionSummaryStream.print();
	}

	@Override
	public DataStream<Query> readQueryStream(ParameterTool params, StreamExecutionEnvironment env) {
		return env.socketTextStream("localhost", 9999).flatMap(new QueryStringParser());
	}

	@Override
	public DataStream<ItemTransaction> readTransactionStream(ParameterTool params, StreamExecutionEnvironment env) {
		return env.addSource(new ItemTransactionGeneratorSource(params));
	}
}
