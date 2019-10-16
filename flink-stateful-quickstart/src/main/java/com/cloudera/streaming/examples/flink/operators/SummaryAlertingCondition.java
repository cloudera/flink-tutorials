/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
