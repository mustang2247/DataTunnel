package com.ganqiang.datatunnel.conf;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;

import org.apache.log4j.Logger;
import org.dom4j.Element;
import org.dom4j.Node;

import com.ganqiang.datatunnel.conf.Task.Pair;
import com.ganqiang.datatunnel.core.Constants;
import com.ganqiang.datatunnel.meta.Visitable;
import com.ganqiang.datatunnel.meta.Visitor;
import com.ganqiang.datatunnel.util.ArithUtil;
import com.ganqiang.datatunnel.util.DateUtil;
import com.ganqiang.datatunnel.util.StringUtil;

public class JobConfigHandler extends AbstractConfig implements Visitable{

	private static final Logger logger = Logger.getLogger(JobConfigHandler.class);

	private final String job_xml = System.getProperty("user.dir")	+ "/conf/datatunnel-job.xml";

	@Override
	String getConfigFile() {
		return job_xml;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void loading() {
		Element jobNode = XmlHelper.getElement(getDocument(), job_node);
		List<Element> taskList = jobNode.selectNodes(task_node);
		for (Element task : taskList) {
			Task subjob = new Task();
			String idvalue = task.attributeValue(id_node);
			String threadNum = task.attributeValue(thread_num_node);
			subjob.setId(idvalue);
			if (!StringUtil.isNullOrBlank(threadNum)){
				subjob.setThreadNum(Integer.valueOf(threadNum.trim()));
			}
			Node timer = task.selectSingleNode(timer_node);
			if (timer != null && timer.hasContent()){
				Node intervalNode = timer.selectSingleNode(interval_node);
				Node startTimeNode = timer.selectSingleNode(start_time_node);
				String interval = intervalNode.getText().trim();
				String startTime = startTimeNode.getText().trim();
				if (!StringUtil.isNullOrBlank(interval) || !StringUtil.isNullOrBlank(startTime)) {
					if (Constants.timer == null){
						Constants.timer = new Timer();
					}
					if (!StringUtil.isNullOrBlank(interval)) {
						subjob.setInterval(Long.valueOf(ArithUtil.parseExp(interval)) * 1000);
					}
					if (!StringUtil.isNullOrBlank(startTime)) {
						subjob.setStartTime(DateUtil.strToDate(startTime));
					}
				}
//				subjob.setTimer(true);
			} else {
//				subjob.setTimer(false);
			}
			
			List<Pair> pairs = new ArrayList<Pair>();
			List<Element> pairList = task.selectNodes(pair_node);
			for(Element elepair : pairList){
				String pairid = elepair.attributeValue(id_node);
				if (StringUtil.isNullOrBlank(pairid)) {
					logger.error("pair id of task ["+idvalue+"] is null");
					System.exit(1);
				}
				Pair pair = new Pair();
				pair.setId(pairid.trim());
				Element readerNode = (Element) elepair.selectSingleNode(reader_node);
				Element writerNode = (Element) elepair.selectSingleNode(writer_node);
				String readerType = readerNode.attributeValue(type_node);
				String readerPoolId = readerNode.attributeValue(pool_id_node);
				if (!Constants.conf_pool_map.containsKey(readerPoolId)) {
					logger.error("["+readerPoolId+"] pool_id does not exist in the pool. ");
					System.exit(1);
				}
				String readerValue = elepair.elementText(reader_node);
				String writerType = writerNode.attributeValue(type_node);
				String writerPoolId = writerNode.attributeValue(pool_id_node);
				if (!Constants.conf_pool_map.containsKey(writerPoolId)) {
					logger.error("["+writerPoolId+"] pool_id does not exist in the pool. ");
					System.exit(1);
				}
				String writerValue = elepair.elementText(writer_node);
				pair.setReaderPoolId(readerPoolId.trim());
				pair.setReaderType(readerType.trim());
				pair.setReaderValue(readerValue.trim());
				pair.setWriterPoolId(writerPoolId.trim());
				pair.setWriterType(writerType.trim());
				pair.setWriterValue(writerValue.trim());
				pairs.add(pair);
			}
			subjob.setPairs(pairs);
			Constants.conf_job_list.add(subjob);
		}
	}
	
	public static void main(String... args) {
		JobConfigHandler conf = new JobConfigHandler();
		conf.loading();
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visitJobConfig(this);
	}
	
}
