package club.hackbook;

import java.util.Set;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName="hackbook_globalvars")
public class GlobalvarItem {

	private String name;
	private String stringval; // one or the other, never both
	private long numberval;  // one or the other, never both
	private Set<String> stringsetval; 
	
	@DynamoDBHashKey(attributeName="name") 
	public String getName() {return name; }
	public void setName(String name) { this.name = name; }
	
	@DynamoDBAttribute(attributeName="stringval")
	public String getStringValue() {return stringval; }
	public void setStringValue(String stringval) { this.stringval = stringval; }
	
	@DynamoDBAttribute(attributeName="numberval")  
	public long getNumberValue() {return numberval; }
	public void setNumberValue(long numberval) { this.numberval = numberval; }
	
	@DynamoDBAttribute(attributeName="stringsetval")
	public Set<String> getStringSetValue() {return stringsetval; }
	public void setStringSetValue(Set<String> stringsetval) { this.stringsetval = stringsetval; }
	
}