package com.cloudera.streaming.examples.flink.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;

import com.cloudera.streaming.examples.flink.types.TransactionSummary;

public class SummaryAlertingCondition implements FilterFunction<TransactionSummary> {

	public static final String REPORTING_NUMBER_KEY = "transaction.num.min";
	public static final String REPORTING_FAILURE_RATE_KEY = "transaction.failure.rate.min";

	private final int minNum;
	private final double minFailureRate;

	public SummaryAlertingCondition(ParameterTool params) {
		minNum = params.getInt(REPORTING_NUMBER_KEY, 100);
		minFailureRate = params.getDouble(REPORTING_FAILURE_RATE_KEY, 0.5);
	}

	@Override
	public boolean filter(TransactionSummary transactionSummary) throws Exception {
		int total = transactionSummary.numSuccessfulTransactions + transactionSummary.numFailedTransactions;
		return total > minNum && (((double) transactionSummary.numFailedTransactions) / total) > minFailureRate;
	}
}