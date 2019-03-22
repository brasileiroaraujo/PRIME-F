/*
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    Copyright (C) 2015 George Antony Papadakis (gpapadis@yahoo.gr)
 */

package DataStructures;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

/**
 *
 * @author gap2
 */
public class EntityProfile implements Serializable {

	private static final long serialVersionUID = 122354534453243447L;

	private final Set<Attribute> attributes;
	private final String entityUrl;
	private boolean isSource;
	private int key;
	private int incrementID;
	private Timestamp creation;
	private Set<Integer> setOfTokens;

	public Object[] getAllValues() {
	    return new Object[]{key, entityUrl, isSource, creation};
	}
	
	private final String split1 = "<<>>";
	private final String split2 = "#=#";

//	public EntityProfile(String url) {
//		entityUrl = url;
//		attributes = new HashSet();
//	}

	/**
	 * Parse out of the format found in generated CSV files
	 * 
	 * @param csvLine
	 *            A comma separated key and values
	 * @throws IllegalArgumentException
	 */
	public EntityProfile(String csvLine, String separator) {
		String[] parts = csvLine.split(separator);
		key = Integer.valueOf(parts[0]);
		entityUrl = parts[1];
		creation = new Timestamp(System.currentTimeMillis());
		attributes = new HashSet();
		for (int i = 1; i < parts.length; i++) {//the first element is the key (avoid!)
			attributes.add(new Attribute("", parts[i]));
		}
	}
	
//	public EntityProfile() {
//		key = -1;
//		entityUrl = "NONE";
//		creation = new Timestamp(System.currentTimeMillis());
//		attributes = new HashSet();
//	}
	
	public EntityProfile(String standardFormat) {
		String[] parts = standardFormat.split(split1);
		isSource = Boolean.parseBoolean(parts[0]);
		entityUrl = parts[1];
		key = Integer.valueOf(parts[2]);
		if (!parts[3].equalsIgnoreCase("incrementID")) {
			incrementID = Integer.valueOf(parts[3]);
		}
		creation = new Timestamp(System.currentTimeMillis());
		attributes = new HashSet();
		setOfTokens = new HashSet<Integer>();
		for (int i = 4; i < parts.length; i++) {//the first element is the key (avoid!)
			String[] nameValue = parts[i].split(split2);
			if (nameValue.length == 1) {
				attributes.add(new Attribute(nameValue[0], ""));
			} else {
				attributes.add(new Attribute(nameValue[0], nameValue[1]));
				setOfTokens.addAll(generateTokens(nameValue[1]));
			}
		}
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public void addAttribute(String propertyName, String propertyValue) {
		attributes.add(new Attribute(propertyName, propertyValue));
	}


	public String getEntityUrl() {
		return entityUrl;
	}

	public int getProfileSize() {
		return attributes.size();
	}

	public Set<Attribute> getAttributes() {
		return attributes;
	}

	public String getStandardFormat() {
		String output = "";
		output += isSource + split1;
		output += entityUrl + split1;//separate the attributes
		output += key + split1;//separate the attributes
		output += incrementID + split1;//separate the attributes
		
		for (Attribute attribute : attributes) {
			output += attribute.getName() + split2 + attribute.getValue() + split1;
		}
				
		return output;
	}
	
	public String getStandardFormat2() {
		String output = "";
		output += isSource + split1;
		output += entityUrl + split1;//separate the attributes
		output += key + split1;//separate the attributes
		output += "incrementID" + split1;//separate the attributes
		
		for (Attribute attribute : attributes) {
			output += attribute.getName() + split2 + attribute.getValue() + split1;
		}
				
		return output;
	}
	
//	public String getStandardFormat2() {
//		String output = "";
//		output += isSource + split1;
////		output += entityUrl + split1;//separate the attributes
//		output += key + split1;//separate the attributes
//		
//		for (Attribute attribute : attributes) {
//			output += attribute.getValue() + " ";
//		}
//				
//		return output;
//	}


	public boolean isSource() {
		return isSource;
	}


	public void setSource(boolean isSource) {
		this.isSource = isSource;
	}

	public Timestamp getCreation() {
		return creation;
	}

	public void setCreation(Timestamp creation) {
		this.creation = creation;
	}
	
	public int getIncrementID() {
		return incrementID;
	}

	public void setIncrementID(int incrementID) {
		this.incrementID = incrementID;
	}
	
	public Set<Integer> getSetOfTokens() {
		return setOfTokens;
	}

	public void setSetOfTokens(Set<Integer> setOfTokens) {
		this.setOfTokens = setOfTokens;
	}

	private Set<Integer> generateTokens(String string) {
		if (string.length() > 20 && string.substring(0, 20).equals("<http://dbpedia.org/")) {
			String[] uriPath = string.split("/");
			string = Arrays.toString(uriPath[uriPath.length-1].split("[\\W_]")).toLowerCase();
		}
		
		//To improve quality, use the following code
		Pattern p = Pattern.compile("[^a-zA-Z\\s0-9]");
		Matcher m = p.matcher("");
		m.reset(string);
		String standardString = m.replaceAll("");
		
		KeywordGenerator kw = new KeywordGeneratorImpl();
		return kw.generateKeyWordsHashCode(standardString);
	}
	
	private String[] generateTokensForBigData(String string) {
		if (string.length() > 20 && string.substring(0, 20).equals("<http://dbpedia.org/")) {
			String[] uriPath = string.split("/");
			string = uriPath[uriPath.length-1];
		}
		
		return string.split("[\\W_]");
	}

	@Override
	public String toString() {
		return "" + getKey() + "-" + isSource();
	}

}