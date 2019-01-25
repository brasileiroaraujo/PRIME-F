package evaluation;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

public class FindingEntity {

	public static void main(String[] args) {
		String INPUT_PATH1 = "inputs/dataset1_amazon";
		String INPUT_PATH2 = "inputs/dataset2_gp";
		
		
		ArrayList<EntityProfile> EntityListSource = null;
		ArrayList<EntityProfile> EntityListTarget = null;
		// reading the files
		ObjectInputStream ois1;
		ObjectInputStream ois2;
		try {
			
			ois1 = new ObjectInputStream(new FileInputStream(INPUT_PATH1));
			ois2 = new ObjectInputStream(new FileInputStream(INPUT_PATH2));
			
			//entity source
			System.out.println("---- Entity 1 ----");
			EntityListSource = (ArrayList<EntityProfile>) ois1.readObject();
			EntityProfile entity1 = EntityListSource.get(722);//lembre de diminuir 1 do id (lista comeca por 0)
//			System.out.println(entity1.getEntityUrl());
			for (Attribute att : entity1.getAttributes()) {
				System.out.println(att.getName() + ": " + att.getValue());
			}
			System.out.println(getTokens(entity1));
			
			System.out.println();
			
			//entity target
			System.out.println("---- Entity 2 ----");
			EntityListTarget = (ArrayList<EntityProfile>) ois2.readObject();
			EntityProfile entity2 = EntityListTarget.get(847);//831,619,829,865,776,896,946,615,642//lembre de diminuir 1 do id (lista comeca por 0)
//			System.out.println(entity2.getEntityUrl());
			for (Attribute att : entity2.getAttributes()) {
				System.out.println(att.getName() + ": " + att.getValue());
			}
			System.out.println(getTokens(entity2));
			
			ois1.close();
			ois2.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private static Set<String> generateTokens(String string) {
		Pattern p = Pattern.compile("[^a-zA-Z\\s0-9]");
		Matcher m = p.matcher("");
		m.reset(string);
		String standardString = m.replaceAll("");
		
		KeywordGenerator kw = new KeywordGeneratorImpl();
		return kw.generateKeyWords(standardString);
	}
	
	private static Set<Integer> getTokens(EntityProfile se) {
		Set<Integer> cleanTokens = new HashSet<Integer>();

		for (Attribute att : se.getAttributes()) {
			KeywordGenerator kw = new KeywordGeneratorImpl();
			for (String string : generateTokens(att.getValue())) {
				cleanTokens.add(string.hashCode());
			}
		}
		
		return cleanTokens;
	}

}
