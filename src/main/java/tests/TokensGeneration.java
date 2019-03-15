package tests;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

public class TokensGeneration {
	public static void main(String[] args) throws URISyntaxException {
//		String s = "Panasonic TH-42PZ80U - 42' Widescreen Panasonic of Panasonic Panasonic 1080p Plasma HDTV - 1000000:1 Dynamic Contrast Ratio";
		String s = "http://dbpedia.org/resource/Philip_Barry";
		
		if (s.substring(0, 19).equals("http://dbpedia.org/")) {
			String[] uriPath = s.split("/");
			s = uriPath[uriPath.length-1];
		}
		
		String[] x = s.split("[\\W_]");
		for (String string : x) {
			System.out.println(string);
		}
//		Pattern p = Pattern.compile("[^a-zA-Z\\s0-9]");
//		Matcher m = p.matcher("");
//		m.reset(s);
//		String standardString = m.replaceAll("");
//		
//		KeywordGenerator kw = new KeywordGeneratorImpl();
//		Set<String> tk = kw.generateKeyWords(standardString);
//		
//		System.out.println(tk);
	}
}
