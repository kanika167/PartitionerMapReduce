package DepartmentPatitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomDepartmentPartitioner extends Partitioner<Text,Text> {

	@Override
	public int getPartition(Text key, Text values, int numOfPartitions) {
		if (key.equals(new Text("Program Department"))) {
			return 0;
		}
		else if(key.equals(new Text("Admin Department"))){
			return 1 % numOfPartitions;
		}
		else if(key.equals(new Text("Accounts Department"))){
			return 2 % numOfPartitions;
		}
		
		return 0 ;
		
	}
	
}
