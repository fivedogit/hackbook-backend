package club.hackbook;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

@DynamoDBTable(tableName="hackbook_chat")
public class ChatItem implements java.lang.Comparable<ChatItem> {
	
	private String id; 
	private String user_id;
	private String text;
	private String hostname;
	private long msfe;
	
	@DynamoDBHashKey(attributeName="id") 
	public String getId() {return id; }
	public void setId(String id) { this.id = id; }
	
	@DynamoDBAttribute(attributeName="user_id") 
	public String getUserId() {return user_id; }
	public void setUserId(String user_id) { this.user_id = user_id; }
	
	@DynamoDBAttribute(attributeName="text") 
	public String getText() {return text; }
	public void setText(String text) { this.text = text; }
	
	@DynamoDBIndexHashKey(attributeName="hostname", globalSecondaryIndexName="hostname-msfe-index")
	public String getHostname() {return hostname; }
	public void setHostname(String hostname) { this.hostname = hostname; }
	
	@DynamoDBIndexRangeKey(attributeName="msfe", globalSecondaryIndexName="hostname-msfe-index")
	public long getMSFE() {return msfe; }
	public void setMSFE(long msfe) { this.msfe = msfe; }
		
	@DynamoDBIgnore
	public JSONObject getJSON()
	{
		JSONObject jo = null;
		try {
			jo = new JSONObject();
			jo.put("id", getId());
			jo.put("user_id", getUserId());
			jo.put("msfe", getMSFE());
			jo.put("text", getText());
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jo;
	}
	
	@DynamoDBIgnore
	public int compareTo(ChatItem o) // this makes more recent comments come first
	{
	    long othermsfe = ((ChatItem)o).getMSFE();
	    if(othermsfe < getMSFE()) // this is to prevent equals
	    	return 1;
	    else if (othermsfe > getMSFE())
	    	return -1;
	    else
	    	return 0;
	}
	
}
