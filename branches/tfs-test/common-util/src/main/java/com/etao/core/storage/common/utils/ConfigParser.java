package com.etao.core.storage.common.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

class ConfigParser {

	private String file;
	private String currentSection;
	private List<String> sections;

	private static Logger logger = Logger.getLogger(ConfigParser.class);
	private Map<String, List<String>> sectionItems = new HashMap<String, List<String>>();

	public ConfigParser(String file) throws IOException {
		this.file = file;
		this.sections = new ArrayList<String>();

		loadConf();
	}

	public String getValue(String section, String key) {
		if (!sections.contains(section)) {
			return null;
		}

		String oldKey = null;
		for (String item : sectionItems.get(section)) {
			oldKey = item.split("=")[0].trim();
			if (oldKey.equals(key)) {
				return item.split("=")[1].trim();
			}
		}

		return null;
	}

	public boolean setValue(String section, Map<String, String> kvMaps) {
		logger.debug("save conf content for section: " + section
				+ " the value is: " + kvMaps);
		if (!sections.contains(section)) {
			sectionItems.put(section, new ArrayList<String>());
			sections.add(section);
		}

		for (String key : kvMaps.keySet()) {
			setValue(section, key, kvMaps.get(key));
		}

		return save();
	}

	private void setValue(String section, String key, String value) {
		List<String> items = sectionItems.get(section);

		String oldKey = null;
		for (int i = 0; i < items.size(); i++) {
			oldKey = items.get(i).split("=")[0].trim();
			if (oldKey.equals(key)) {
				items.set(i, key + "=" + value);
				return;
			}
		}

		items.add(key + "=" + value);
	}

	public boolean commentConfItem(String section, String keyword) {
		if (!sectionItems.containsKey(section)) {
			logger.error("can not find section: [" + section + "]");
			return false;
		}

		String item = null;
		List<String> items = sectionItems.get(section);
		for (int i = 0; i < items.size(); i++) {
			item = items.get(i);
			if (!item.startsWith("#") && item.contains(keyword)) {
				items.set(i, "#" + item);
				return save();
			}
		}
		logger.error("can not find keyword: " + keyword + " in section: "
				+ section);
		return false;
	}

	public boolean unCommentConfItem(String section, String keyword) {
		if (!sectionItems.containsKey(section)) {
			logger.error("can not find section: [" + section + "]");
			return false;
		}

		String item = null;
		List<String> items = sectionItems.get(section);
		for (int i = 0; i < items.size(); i++) {
			item = items.get(i);
			if (item.startsWith("#") && item.contains(keyword)) {
				items.set(i, item.substring(1));
				return save();
			}
		}
		logger.error("can not find keyword: " + keyword + "in section: "
				+ section);
		return false;
	}

	public boolean save() {
		FileWriter fw = null;
		BufferedWriter writer = null;

		try {
			fw = new FileWriter(file);
			writer = new BufferedWriter(fw);

			for (String section : sections) {
				writer.write("[" + section + "]");
				writer.newLine();
				for (String item : sectionItems.get(section)) {
					writer.write(item);
					writer.newLine();
				}
				writer.newLine();
			}
			writer.flush();
			writer.close();
			fw.close();
		} catch (IOException e) {
			logger.error(e.getMessage());
			return false;
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					logger.error(e.getMessage());
				}
			}

			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					logger.error(e.getMessage());
				}
			}
		}

		return true;
	}

	protected void loadConf() throws IOException {
		String line = null;
		FileReader fr = null;
		BufferedReader reader = null;

		try {
			fr = new FileReader(file);
			reader = new BufferedReader(fr);

			while ((line = reader.readLine()) != null) {
				if (!parseLine(line)) {
					reader.close();
					fr.close();
					break;
				}
			}
			reader.close();
			fr.close();
		} catch (IOException e) {
			logger.error("IOException: " + e.getMessage());
			throw e;
		} finally {
			if (reader != null) {
				reader.close();
			}

			if (fr != null) {
				fr.close();
			}
		}
	}

	protected boolean parseLine(String line) {
		line = line.trim();
		if (line.matches("^\\[.*\\]$")) {
			currentSection = line.replaceFirst("\\[(.*)\\]", "$1");
			currentSection = currentSection.trim();
			if (sections.contains(currentSection)) {
				return false;
			} else {
				sections.add(currentSection);
				sectionItems.put(currentSection, new ArrayList<String>());
			}
		} else if (!line.equals("")) {
			sectionItems.get(currentSection).add(line);
		}
		return true;
	}
}
