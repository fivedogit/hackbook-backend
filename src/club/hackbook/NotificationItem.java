package club.hackbook;


import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

// TYPES
// 0. ** Someone followed user
// 1. ** a comment user wrote was upvoted
// 2. ** a comment user wrote was downvoted
// 3. ** a story user wrote was upvoted
// 4. ** a story user wrote was downvoted
// 5. ** a comment user wrote was commented on
// 6. ** a story user wrote was commented on
// 7. a user this user is following posted a story with a url // should be combined with 9?
// 8. a user this user is following commented
// ******* DELETED, COMBINED WITH 7 ********** 9. a user this user is following posted a story without a url // should be combined with 7?
// A. ?? someone deep-replied to your comment
// B. ?? someone deep-replied to your story



@DynamoDBTable(tableName="hackbook_notifications")
public class NotificationItem implements java.lang.Comparable<NotificationItem> {
	
	private String id; 
	private String user_id;
	private long action_msfe;
	private long msfe;
	private String type;
	private long hn_target_id;
	private String triggerer;
	private long hn_root_id;
	private int karma_change;
	
	@DynamoDBHashKey(attributeName="id") 
	public String getId() {return id; }
	public void setId(String id) { this.id = id; }
	
	@DynamoDBIndexHashKey(attributeName="user_id", globalSecondaryIndexName="user_id-msfe-index") 
	public String getUserId() {return user_id; }
	public void setUserId(String user_id) { this.user_id = user_id; }
	
	@DynamoDBAttribute(attributeName="action_msfe") 
	public long getActionMSFE() {return action_msfe; }
	public void setActionMSFE(long action_msfe) { this.action_msfe = action_msfe; }
	
	@DynamoDBIndexRangeKey(attributeName="msfe", globalSecondaryIndexName="user_id-msfe-index")
	public long getMSFE() {return msfe; }
	public void setMSFE(long msfe) { this.msfe = msfe; }
	
	@DynamoDBAttribute(attributeName="type") 
	public String getType() {return type; }
	public void setType(String type) { this.type = type; }
	
	@DynamoDBAttribute(attributeName="hn_target_id") 
	public long getHNTargetId() {return hn_target_id; }
	public void setHNTargetId(long hn_target_id) { this.hn_target_id = hn_target_id; }
	
	@DynamoDBAttribute(attributeName="triggerer") 
	public String getTriggerer() {return triggerer; }
	public void setTriggerer(String triggerer) { this.triggerer = triggerer; }
	
	@DynamoDBAttribute(attributeName="hn_root_id") 
	public long getHNRootId() {return hn_root_id; }
	public void setHNRootId(long hn_root_id) { this.hn_root_id = hn_root_id; }
	
	@DynamoDBAttribute(attributeName="karma_change") 
	public int getKarmaChange() {return karma_change; }
	public void setKarmaChange(int karma_change) { this.karma_change = karma_change; }
	
	@DynamoDBIgnore
	public JSONObject getJSON()
	{
		JSONObject jo = null;
		try {
			jo = new JSONObject();
			jo.put("id", getId());
			jo.put("user_id", getUserId());
			jo.put("action_msfe", getActionMSFE());
			jo.put("msfe", getMSFE());
			jo.put("type", getType());
			jo.put("hn_target_id", getHNTargetId());
			jo.put("triggerer", getTriggerer());
			jo.put("hn_root_id", getHNRootId());
			jo.put("karma_change", getKarmaChange());
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jo;
	}
	
	@DynamoDBIgnore
	public int compareTo(NotificationItem o) // this makes more recent comments come first
	{
	    long otheractionmsfe = ((NotificationItem)o).getActionMSFE();
	    if(otheractionmsfe < getActionMSFE()) // this is to prevent equals
	    	return 1;
	    else if (otheractionmsfe > getActionMSFE())
	    	return -1;
	    else
	    	return 0;
	}
	
}
