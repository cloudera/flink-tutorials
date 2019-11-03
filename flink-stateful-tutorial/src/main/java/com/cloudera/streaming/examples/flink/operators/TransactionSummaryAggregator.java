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

import org.apache.flink.api.common.functions.AggregateFunction;

import com.cloudera.streaming.examples.flink.types.TransactionResult;
import com.cloudera.streaming.examples.flink.types.TransactionSummary;

public class TransactionSummaryAggregator implements AggregateFunction<TransactionResult, TransactionSummary, TransactionSummary> {

	@Override
	public TransactionSummary createAccumulator() {return new TransactionSummary();}

	@Override
	public TransactionSummary add(TransactionResult tr, TransactionSummary acc) {
		acc.itemId = tr.transaction.itemId;
		if (tr.success) {
			acc.numSuccessfulTransactions++;
		} else {
			acc.numFailedTransactions++;
		}
		acc.totalVolume += Math.abs(tr.transaction.quantity);
		return acc;
	}

	@Override
	public TransactionSummary getResult(TransactionSummary acc) {return acc;}

	@Override
	public TransactionSummary merge(TransactionSummary ts1, TransactionSummary ts2) {
		return new TransactionSummary(ts2.itemId != null ? ts2.itemId : ts1.itemId,
				ts1.numFailedTransactions + ts2.numFailedTransactions,
				ts1.numSuccessfulTransactions + ts2.numSuccessfulTransactions,
				ts1.totalVolume + ts2.totalVolume);
	}
}
