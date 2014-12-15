package com.ganqiang.datatunnel.meta.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.ganqiang.datatunnel.core.Constants;

public class HBaseExecutor {
	
	private String poolId;
	
	public HBaseExecutor(String poolId){
		this.poolId = poolId;
	}
	
	private static final Logger logger = Logger.getLogger(HBaseExecutor.class);

	public Map<String, String> find(String tablename, String rowKey) {
		HBaseConnController hcc = Constants.hbase_conn_map.get(poolId);
		Map<String,String> rowData = new HashMap<String,String>();
		HTableInterface table = null;
		try {
			table = hcc.getHTableInterface(tablename);
			Get get = new Get(rowKey.getBytes());
			Result rs = table.get(get);
			for (Cell cell : rs.rawCells()) {
				String column = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell));
				rowData.put(column, value);
			}
		}catch(Exception e){
			logger.error("Cannot to excute find method by hbase  ", e);
		}finally{
			try {
				if (table != null){
					table.close();
				}
			} catch (IOException e) {
				logger.error("Cannot to excute find method by hbase  ", e);
			}
		}
		return rowData;
	}

	public List<Map<String, String>> find(String tablename) {
		HBaseConnController hcc = Constants.hbase_conn_map.get(poolId);
		List<Map<String,String>> list = new ArrayList<Map<String,String>>();
		HTableInterface table = null;
		try {
			table = hcc.getHTableInterface(tablename);
			ResultScanner rs = table.getScanner(new Scan()); 
			for (Result result : rs) {
				Map<String,String> rowData = new HashMap<String,String>();
				for (Cell cell : result.rawCells()) {
					String column = new String(CellUtil.cloneQualifier(cell));
					String value = new String(CellUtil.cloneValue(cell));
					rowData.put(column, value);
				}
				list.add(rowData);
			}
		}catch(Exception e){
			logger.error("Cannot to excute find method by hbase  ", e);
		}finally{
			try {
				if (table != null){
					table.close();
				}
			} catch (IOException e) {
				logger.error("Cannot to excute find method by hbase  ", e);
			}
		}
		return list;
	}

	public List<Map<String, String>> find(String tablename, Filter filter) {
		HBaseConnController hcc = Constants.hbase_conn_map.get(poolId);
		List<Map<String,String>> list = new ArrayList<Map<String,String>>();
		HTableInterface table = null;
		try {
			table = hcc.getHTableInterface(tablename);
			Scan s = new Scan(); 
			s.setFilter(filter);
			ResultScanner rs = table.getScanner(s); 
			for (Result result : rs) {
				Map<String,String> rowData = new HashMap<String,String>();
				for (Cell cell : result.rawCells()) {
					String column = new String(CellUtil.cloneQualifier(cell));
					String value = new String(CellUtil.cloneValue(cell));
					rowData.put(column, value);
				}
				list.add(rowData);
			}
		}catch(Exception e){
			logger.error("Cannot to excute find method by hbase  ", e);
		}finally{
			try {
				if (table != null){
					table.close();
				}
			} catch (IOException e) {
				logger.error("Cannot to excute find method by hbase  ", e);
			}
		}
		return list;
	}
	
	
	public void putBatch(String tablename,List<Put> putlist) {
		HBaseConnController hcc = Constants.hbase_conn_map.get(poolId);
		HTableInterface table = null;
		try {
			table = hcc.getHTableInterface(tablename);
			table.put(putlist);
		} catch (IOException e) {
			logger.error("Cannot to excute writeBatch method by hbase ", e);
		} finally {
			try {
				if (table != null) {
					table.close();
				}
			} catch (IOException e) {
				logger.error("Cannot to excute writeBatch method by hbase  ", e);
			}
		}
	}
	
	public void deleteBatch(String tablename,List<String> rowkeylist) {
		HBaseConnController hcc = Constants.hbase_conn_map.get(poolId);
		HTableInterface table = null;
		try {
			table = hcc.getHTableInterface(tablename);
			List<Delete> list = new ArrayList<Delete>();
			for(String rowkey : rowkeylist){
				Delete d1 = new Delete(rowkey.getBytes()); 
				list.add(d1);
			}
    table.delete(list);
		} catch (IOException e) {
			logger.error("Cannot to excute writeBatch method by hbase ", e);
		} finally {
			try {
				if (table != null) {
					table.close();
				}
			} catch (IOException e) {
				logger.error("Cannot to excute writeBatch method by hbase  ", e);
			}
		}
	}
	

	public static void main(String... args){
		HBaseExecutor he = new HBaseExecutor("poolid");
		
		//单个column条件查询
//		Filter f = new SingleColumnValueFilter(Bytes 
//                .toBytes("abc"), Bytes.toBytes("key"), CompareOp.EQUAL, Bytes 
//                .toBytes("value1"));
		
		//多个column条件查询
		List<Filter> filters = new ArrayList<Filter>(); 
		Filter filter1 = new SingleColumnValueFilter(Bytes .toBytes("abc"),null, CompareOp.EQUAL, Bytes .toBytes("value1")); 
   filters.add(filter1); 
   Filter filter2 = new SingleColumnValueFilter(Bytes .toBytes("abc"), Bytes.toBytes("key"), CompareOp.EQUAL, Bytes .toBytes("123123")); 
   filters.add(filter2); 
   Filter f = new FilterList(filters);
		
		List<Map<String, String>> list = he.find("test1", f);
		for (Map<String, String> map : list) {
			System.out.println(" ------------------ ");
			for (String key : map.keySet()) {
				System.out.println(key+" ==== "+map.get(key));
			}
		}

//       更新数据        		
		List<Put> list2 = new ArrayList<Put>();
		for (int i=0; i<2; i++) {
			Put p = new Put(Bytes.toBytes("123"));
			p.add(Bytes.toBytes("abc"), Bytes.toBytes("NAME"), Bytes.toBytes("aaaaa"));
			p.add(Bytes.toBytes("abc"),Bytes.toBytes("BORROWER"),Bytes.toBytes("bbbb"));
			list2.add(p);
		}
		he.putBatch("test1", list2);
		
//  删除数据		
		List<String> strlist = new ArrayList<String>();
		strlist.add("row1");
		strlist.add("123");
		he.deleteBatch("test1", strlist);
	}

}
