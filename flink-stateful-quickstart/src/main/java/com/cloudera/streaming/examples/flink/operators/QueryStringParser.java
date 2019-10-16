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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import com.cloudera.streaming.examples.flink.types.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryStringParser implements FlatMapFunction<String, Query> {

	private static final Logger LOG = LoggerFactory.getLogger(QueryStringParser.class);

	@Override
	public void flatMap(String queryString, Collector<Query> out) throws Exception {
		try {
			String[] split = queryString.split(" ");
			out.collect(new Query(Long.parseLong(split[0]), split[1]));
		} catch (Exception parseErr) {
			LOG.error("Could not parse item query: {}", queryString);
		}
	}
}
