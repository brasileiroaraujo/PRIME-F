package DataProducer;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import info.debatty.java.lsh.MinHash;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

//localhost:9092 20 inputs/dataset1_abt true 0.1
public class KafkaDataStreamingProducerByTimeAttSelection {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);//localhost:9092    10.171.171.50:8088
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
//        int[] timers = {10, 25, 50, 100, 250};
        int timer = Integer.parseInt(args[1]) * 1000;//second to milisecond
        Random random = new Random();
        
        //TOPIC
        final String topicName = "mytopic";
        
        //CHOOSE THE INPUT PATHS
        String INPUT_PATH1 = args[2];
        String INPUT_PATH2 = args[3];
        boolean IS_SOURCE = Boolean.parseBoolean(args[4]);
        double percentageOfEntitiesPerIncrement = Double.parseDouble(args[5]); //number of entities per increment based on the percentage. e.g: 0,1 is 10% 
        int currentIncrement = 0;
        
        int currentTokenID = 0;
        HashMap<String, Integer> tokensIDs = new HashMap<String, Integer>();
        HashMap<String, Set<Integer>> attributeMapSource = new HashMap<String, Set<Integer>>();
        HashMap<String, Set<Integer>> attributeMapTarget = new HashMap<String, Set<Integer>>();
        Set<String> attributesSource = new HashSet<String>();
        ArrayList<EntityProfile> entitiesAfterAttSelection = new ArrayList<EntityProfile>();
        

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ArrayList<EntityProfile> EntityListSource = null;
        ArrayList<EntityProfile> EntityListTarget = null;
        
		// reading the files
		ObjectInputStream ois1;
		ObjectInputStream ois2;
		try {
			ois1 = new ObjectInputStream(new FileInputStream(INPUT_PATH1));
			ois2 = new ObjectInputStream(new FileInputStream(INPUT_PATH2));
			EntityListSource = (ArrayList<EntityProfile>) ois1.readObject();
			EntityListTarget = (ArrayList<EntityProfile>) ois2.readObject();
			ois1.close();
			ois2.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int incrementControlerSource = (int)Math.ceil(percentageOfEntitiesPerIncrement * EntityListSource.size());
		int incrementControlerTarget = (int)Math.ceil(percentageOfEntitiesPerIncrement * EntityListTarget.size());
		int uniqueIdSource = 0;
		int uniqueIdTarget = 0;
		
//		for (int i = 0; i < Math.max(EntityListSource.size(), EntityListTarget.size()); i++) {
		while (uniqueIdSource < EntityListSource.size()) {
			currentIncrement++;
			
			//attribute extraction for source
			for (int i = uniqueIdSource; i < incrementControlerSource; i++) {
				if (i < EntityListSource.size()) {
					EntityProfile entitySource = EntityListSource.get(i);
					entitySource.setSource(true);
					entitySource.setKey(uniqueIdSource);
					entitySource.setIncrementID(currentIncrement);
					
					for (Attribute att : entitySource.getAttributes()) {
						KeywordGenerator kw = new KeywordGeneratorImpl();
						for (String tk : generateTokens(att.getValue())) {
							if (!tokensIDs.containsKey(tk)) {
								tokensIDs.put(tk, currentTokenID++);
							}
							
							if (attributeMapSource.containsKey(att.getName())) {
								attributeMapSource.get(att.getName()).add(tokensIDs.get(tk));
							} else {
								attributeMapSource.put(att.getName(), new HashSet<Integer>());
								attributeMapSource.get(att.getName()).add(tokensIDs.get(tk));
							}
						}
					}
				}
				uniqueIdSource++;
			}
			
			//attribute extraction for target
			for (int i = uniqueIdTarget; i < incrementControlerTarget; i++) {
				if (i < EntityListTarget.size()) {
					EntityProfile entityTarget = EntityListTarget.get(i);
					entityTarget.setSource(false);
					entityTarget.setKey(uniqueIdTarget);
					entityTarget.setIncrementID(currentIncrement);
					
					for (Attribute att : entityTarget.getAttributes()) {
						KeywordGenerator kw = new KeywordGeneratorImpl();
						for (String tk : generateTokens(att.getValue())) {
							if (!tokensIDs.containsKey(tk)) {
								tokensIDs.put(tk, currentTokenID++);
							}
							
							if (attributeMapTarget.containsKey(att.getName())) {
								attributeMapTarget.get(att.getName()).add(tokensIDs.get(tk));
							} else {
								attributeMapTarget.put(att.getName(), new HashSet<Integer>());
								attributeMapTarget.get(att.getName()).add(tokensIDs.get(tk));
							}
						}
					}
				}
				uniqueIdTarget++;
			}
			
			//generate the MinHash
			HashMap<String, int[]> attributeHashesSource = new HashMap<String, int[]>();
			HashMap<String, int[]> attributeHashesTarget = new HashMap<String, int[]>();
			MinHash minhash = new MinHash(120, tokensIDs.size());//Estimate the signature size after
			for (String att : attributeMapSource.keySet()) {
				attributeHashesSource.put(att, minhash.signature(attributeMapSource.get(att)));
			}
			for (String att : attributeMapTarget.keySet()) {
				attributeHashesTarget.put(att, minhash.signature(attributeMapTarget.get(att)));
			}
			
			//generate the similarity matrix
			ArrayList<String> attFromSource = new ArrayList<String>(attributeHashesSource.keySet());
			ArrayList<String> attFromTarget = new ArrayList<String>(attributeHashesTarget.keySet());
			double[][] similarityMatrix = new double[attFromSource.size()][attFromTarget.size()];
			double similaritySum = 0;
			for (int i = 0; i < attFromSource.size(); i++) {
				for (int j = 0; j < attFromTarget.size(); j++) {
					double similarity = minhash.similarity(attributeHashesSource.get(attFromSource.get(i)), attributeHashesTarget.get(attFromTarget.get(j)));
					similaritySum += similarity;
					similarityMatrix[i][j] = similarity;
				}
			}
			
			//attribute selection (based on the matrix evaluation)
			double average = similaritySum/(attFromSource.size() * attFromTarget.size());
			ArrayList<Integer> blackListSource = new ArrayList<Integer>();
			ArrayList<Integer> blackListTarget = new ArrayList<Integer>();
			for (int i = 0; i < attFromSource.size(); i++) {
				int count = 0;
				for (int j = 0; j < attFromTarget.size(); j++) {
					if (similarityMatrix[i][j] >= average) {
						count++;
					}
				}
				if (count == 0) {
					blackListSource.add(i);
				}
				count = 0;
			}			
			for (int i = 0; i < attFromTarget.size(); i++) {
				int count = 0;
				for (int j = 0; j < attFromSource.size(); j++) {
					if (similarityMatrix[j][i] >= average) {
						count++;
					}
				}
				if (count == 0) {
					blackListTarget.add(i);
				}
				count = 0;
			}
			
			//removing the bad attributes
			for (EntityProfile e : EntityListSource) {
				for (Integer index : blackListSource) {
					e.getAttributes().remove(attFromSource.get(index));
				}
				entitiesAfterAttSelection.add(e);
			}
			for (EntityProfile e : EntityListTarget) {
				for (Integer index : blackListTarget) {
					e.getAttributes().remove(attFromTarget.get(index));//CORRIGIR ISSO AQUI N VAI REMOVER UM ATTRIBUTO PELO NOME
				}
				entitiesAfterAttSelection.add(e);
			}
				
			
			//Send the entities (without 'bad' attributes)
			for (EntityProfile entityProfile : entitiesAfterAttSelection) {
				ProducerRecord<String, String> record = new ProducerRecord<>(topicName, entityProfile.getStandardFormat());
	            producer.send(record);
			}
			
			incrementControlerSource += (int)Math.ceil(percentageOfEntitiesPerIncrement * EntityListSource.size());
			incrementControlerTarget += (int)Math.ceil(percentageOfEntitiesPerIncrement * EntityListTarget.size());
			System.out.println("Increment " + currentIncrement + " sent.");
			Thread.sleep(timer);
		}
		
		
//		ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "THEEND");
//        producer.send(record);
		
        producer.close();
        System.out.println("----------------------------END------------------------------");
        Calendar data = Calendar.getInstance();
        int horas = data.get(Calendar.HOUR_OF_DAY);
        int minutos = data.get(Calendar.MINUTE);
        int segundos = data.get(Calendar.SECOND);
        System.out.println(horas + ":" + minutos + ":" + segundos);
        
//        throw new Exception();
    }
    
    private static Set<String> generateTokens(String string) {
		Pattern p = Pattern.compile("[^a-zA-Z\\s0-9]");
		Matcher m = p.matcher("");
		m.reset(string);
		String standardString = m.replaceAll("");
		
		KeywordGenerator kw = new KeywordGeneratorImpl();
		return kw.generateKeyWords(standardString);
	}
}
