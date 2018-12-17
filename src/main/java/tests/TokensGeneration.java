package tests;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

public class TokensGeneration {
	public static void main(String[] args) {
		String s = "Panasonic TH-42PZ80U - 42' Widescreen Panasonic Panasonic Panasonic 1080p Plasma HDTV - 1000000:1 Dynamic Contrast Ratio";
		
		Pattern p = Pattern.compile("[^a-zA-Z\\s0-9]");
		Matcher m = p.matcher("");
		m.reset(s);
		String standardString = m.replaceAll("");
		
		KeywordGenerator kw = new KeywordGeneratorImpl();
		Set<String> tk = kw.generateKeyWords(standardString);
		
		System.out.println(tk);
	}
}
