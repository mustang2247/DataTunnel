package com.bytegriffin.datatunnel.conf;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.google.common.collect.Lists;

public class TasksDefineXmlParser {

	private static final Logger logger = LogManager.getLogger(TasksDefineXmlParser.class);
	private static final String conf_path = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
	private final static String taskdefine_xml_file = conf_path + "tasks-define.xml";
	private static final String task_node = "task";
	private static final String name_node = "name";
	private static final String operation_node = "operation";
	private static final String timer_start_node = "timer_start";
	private static final String timer_interval_node = "timer_interval";
	private static final String readers_node = "readers";
	private static final String reader_node = "reader";
	private static final String writers_node = "writers";
	private static final String writer_node = "writer";
	private static final String type_node = "type";
	private static final String address_node = "address";
	private static final String value_node = "value";

	public List<TaskDefine> load() {
		Document doc = loadXML(taskdefine_xml_file);
		List<TaskDefine> list = Lists.newArrayList();
		if (doc == null) {
			return null;
		}
		Element tasksNode = doc.getRootElement();
		if (tasksNode == null) {
			return null;
		}
		List<Element> taskElements = tasksNode.elements(task_node);
		taskElements.forEach(subElement -> {
			if (subElement == null) {
				return;
			}
			TaskDefine conf = new TaskDefine();
			conf.setName(subElement.element(name_node).getStringValue());
			conf.setOperation(subElement.element(operation_node).getStringValue());
			conf.setStartTime(subElement.element(timer_start_node).getStringValue());
			conf.setInterval(subElement.element(timer_interval_node).getStringValue());
			List<Element> readers = subElement.elements(readers_node);
			List<OperatorDefine> readerlist = Lists.newArrayList();
			readers.forEach(ele -> {
				if (ele == null) {
					return;
				}
				Element readerEle = ele.element(reader_node);
				OperatorDefine reader = new OperatorDefine();
				reader.setId(OperatorDefine.reader_opt_id);
				reader.setName(conf.getName());
				reader.setType(readerEle.element(type_node).getStringValue());
				reader.setAddress(readerEle.element(address_node).getStringValue());
				reader.setValue(readerEle.element(value_node).getStringValue());
				readerlist.add(reader);
			});
			conf.setReaderDefines(readerlist);

			List<Element> writers = subElement.elements(writers_node);
			List<OperatorDefine> writerlist = Lists.newArrayList();
			writers.forEach(ele -> {
				if (ele == null) {
					return;
				}
				Element writerEle = ele.element(writer_node);
				OperatorDefine writer = new OperatorDefine();
				writer.setId(OperatorDefine.writer_opt_id);
				writer.setName(conf.getName());
				writer.setType(writerEle.element(type_node).getStringValue());
				writer.setAddress(writerEle.element(address_node).getStringValue());
				writer.setValue(writerEle.element(value_node).getStringValue());
				writerlist.add(writer);
			});
			conf.setWriterDefines(writerlist);
			list.add(conf);
		});
		logger.info("配置文件[{}]读取完成.", taskdefine_xml_file);
		return list;
	}

	private static Document loadXML(String filePath) {
		try {
			SAXReader saxReader = new SAXReader();
			return saxReader.read(filePath);
		} catch (Exception e) {
			logger.error("配置文件[{}]读取失败",taskdefine_xml_file);
		}
		return null;
	}
}
