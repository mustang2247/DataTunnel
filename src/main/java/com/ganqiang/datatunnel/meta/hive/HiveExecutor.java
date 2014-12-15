package com.ganqiang.datatunnel.meta.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.tomcat.jdbc.pool.DataSource;

import com.ganqiang.datatunnel.core.Constants;
import com.ganqiang.datatunnel.meta.db.DBHelper;

public class HiveExecutor {

	private static final Logger logger = Logger.getLogger(HiveExecutor.class);

	private Connection con;

	public HiveExecutor(String poolId) {
		DataSource ds = Constants.hive_datasource_map.get(poolId);
		con = getConnection(ds);
	}

	private Connection getConnection(DataSource dataSource) {
		Connection conn = null;
		try {
			Future<Connection> future = dataSource.getConnectionAsync();
			while (!future.isDone()) {
				logger.info("Job "
						+ Thread.currentThread().getName()
						+ " connection is not yet available.It will auto sleep for 1 ms.");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException x) {
					logger.error("Job " + Thread.currentThread().getName()
							+ " auto sleep is blocked.", x);
				}
			}
			conn = future.get();
		} catch (Exception e) {
			logger.error("Task " + Thread.currentThread().getName() + " hive connection is fault.", e);
		}
		return conn;
	}

	public List<Map<String, Object>> find(final String hql) {
		Map<String, Object> rowData = null;
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Statement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = con.createStatement();
			rs = pstmt.executeQuery(hql);
			ResultSetMetaData md = rs.getMetaData();
			int columnCount = md.getColumnCount();
			while (rs.next()) {
				rowData = new HashMap<String, Object>(columnCount);
				for (int i = 1; i <= columnCount; i++) {
					rowData.put(md.getColumnLabel(i).toUpperCase(),
							rs.getObject(i));
				}
				list.add(rowData);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Cannot to excute find method by hql : " + hql, e);
		} finally {
			DBHelper.close(con, rs, pstmt);
		}
		return list;
	}

	public void execute(final String hql) {
		Statement stmt = null;
		ResultSet rs = null;
		try {
			DBHelper.openTransaction(con);
			stmt = con.createStatement();
			stmt.execute(hql);
			DBHelper.commit(con);
		} catch (SQLException e) {
			DBHelper.rollback(con);
			logger.error("Cannot to excute insert method by hql : " + hql, e);
		} finally {
			DBHelper.close(con, rs, stmt);
		}
	}
}
