package evaluation;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;

import DataStructures.Attribute;
import DataStructures.EntityProfile;

public class FindingEntity {

	public static void main(String[] args) {
		String INPUT_PATH1 = "inputs/dataset1_abt";
		String INPUT_PATH2 = "inputs/dataset2_buy";
		
		
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
			EntityProfile entity1 = EntityListSource.get(475);//lembre de diminuir 1 do id (lista comeca por 0)
//			System.out.println(entity1.getEntityUrl());
			for (Attribute att : entity1.getAttributes()) {
				System.out.println(att.getName() + ": " + att.getValue());
			}
			
			System.out.println();
			
			//entity target
			System.out.println("---- Entity 2 ----");
			EntityListTarget = (ArrayList<EntityProfile>) ois2.readObject();
			EntityProfile entity2 = EntityListTarget.get(619);//831,619,829,865,776,896,946,615,642//lembre de diminuir 1 do id (lista comeca por 0)
//			System.out.println(entity2.getEntityUrl());
			for (Attribute att : entity2.getAttributes()) {
				System.out.println(att.getName() + ": " + att.getValue());
			}
			
			ois1.close();
			ois2.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
