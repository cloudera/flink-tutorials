package com.cloudera.streaming.examples.flink.operators;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import com.cloudera.streaming.examples.flink.types.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Enriches the item info by fetching the item name by item id.
 */
public class ItemInfoEnrichment extends RichAsyncFunction<QueryResult, QueryResult> {

	private static final Logger LOG = LoggerFactory.getLogger(ItemInfoEnrichment.class);

	private static final String ITEM_QUERY = "SELECT name FROM items WHERE itemId = ?;";

	private final int threadPoolSize;
	private final String dbConnectionString;

	private transient Connection dbConnection;
	private transient ExecutorService executor;
	private transient PreparedStatement itemQuery;

	public ItemInfoEnrichment(int threadPoolSize, String dbConnectionString) {
		this.threadPoolSize = threadPoolSize;
		this.dbConnectionString = dbConnectionString;
	}

	@Override
	public void asyncInvoke(QueryResult queryResult, ResultFuture<QueryResult> resultFuture) throws Exception {
		executor.submit(() -> {
			try {
				itemQuery.setString(1, queryResult.itemInfo.itemId);
				ResultSet rs = itemQuery.executeQuery();
				if (rs.next()) {
					queryResult.itemInfo.setItemName(rs.getString("name"));
				}

				resultFuture.complete(Collections.singletonList(queryResult));
			} catch (SQLException t) {
				resultFuture.completeExceptionally(t);
			}
		});
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		executor = Executors.newFixedThreadPool(threadPoolSize);
		Class.forName("com.mysql.jdbc.Driver");
		dbConnection = DriverManager.getConnection(dbConnectionString);
		itemQuery = dbConnection.prepareStatement(ITEM_QUERY);
	}

	@Override
	public void close() {
		try {
			executor.shutdownNow();
		} catch (Throwable t) {
			LOG.error("Error while shutting down executor service.", t);
		}

		try {
			itemQuery.close();
		} catch (Throwable t) {
			LOG.error("Error while closing connection.", t);
		}

		try {
			dbConnection.close();
		} catch (Throwable t) {
			LOG.error("Error while closing connection.", t);
		}
	}
}
