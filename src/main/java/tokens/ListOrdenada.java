package tokens;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeSet;

import DataStructures.TupleSimilarity;
import scala.Tuple2;

public class ListOrdenada {

	public static void main(String[] args) {
		Set<TupleSimilarity> l = new TreeSet<TupleSimilarity>();
		l.add(new TupleSimilarity(5, 0.1));
		l.add(new TupleSimilarity(1, 0.4));
		l.add(new TupleSimilarity(8, 0.9));
//		l.pollFirst();
		
		for (TupleSimilarity integer : l) {
			System.out.println(integer);
		}

	}

}
