package club.hackbook;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIgnore;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

@DynamoDBTable(tableName="hackbook_users2")
public class UserItem implements java.lang.Comparable<UserItem> {

	// static parts of the database entry
	private String id;
	private long since;
	private long seen;
	private String since_hr;
	private String seen_hr;
	private int notification_count;
	private Set<String> notification_ids;
	private int newsfeed_count;
	private Set<String> newsfeed_ids;
	private String this_access_token; 
	private long this_access_token_expires;
	private String permission_level; 
	private int hn_karma;		   // this is set on login and every 20 minutes by getUserSelf
	private int karma_pool;				// rather than produce a NotificationItem every 30 seconds, let's wait some period of time
	private long last_karma_pool_drain; // pool the karma changes together, then unload it all at once.
	private int karma_pool_ttl_mins;
	private long hn_since;
	private String hn_about;
	private String hn_topcolor;
	private String url_checking_mode;
	private String notification_mode;
	private String hn_authtoken;
	private boolean registered;
	private Set<String> followers;
	private Set<String> following;
	private boolean hide_embedded_counts;
	private boolean hide_inline_follow;
	private boolean hide_deep_reply_notifications;
	private boolean hide_promo_links;
	private String ext_version;
	
	
	@DynamoDBHashKey(attributeName="id") 
	public String getId() {return id; }
	public void setId(String id) { this.id = id; }

	@DynamoDBAttribute(attributeName="permission_level")  
	public String getPermissionLevel() {return permission_level; }
	public void setPermissionLevel(String permission_level) { this.permission_level = permission_level; }
	
	@DynamoDBAttribute(attributeName="since")  
	public long getSince() {return since; }
	public void setSince(long since) { this.since = since; }
	
	@DynamoDBAttribute(attributeName="hn_since")  
	public long getHNSince() {return hn_since; }
	public void setHNSince(long hn_since) { this.hn_since = hn_since; }
	
	@DynamoDBAttribute(attributeName="seen")  
	@DynamoDBIndexRangeKey(attributeName="seen", globalSecondaryIndexName="registered-seen-index")
	public long getSeen() {return seen; }
	public void setSeen(long seen) { this.seen = seen; }
	
	@DynamoDBAttribute(attributeName="this_access_token")  
	public String getThisAccessToken() {return this_access_token; }
	public void setThisAccessToken(String this_access_token) { this.this_access_token = this_access_token; }
	
	@DynamoDBAttribute(attributeName="this_access_token_expires")  
	public long getThisAccessTokenExpires() {return this_access_token_expires; }
	public void setThisAccessTokenExpires(long this_access_token_expires) { this.this_access_token_expires = this_access_token_expires; }

	@DynamoDBAttribute(attributeName="notification_count")  
	public int getNotificationCount() { return notification_count; }
	public void setNotificationCount(int notification_count) { this.notification_count = notification_count; }
	
	@DynamoDBAttribute(attributeName="notification_ids")  
	public Set<String> getNotificationIds() { return notification_ids; }
	public void setNotificationIds(Set<String> notification_ids) { this.notification_ids = notification_ids; }
	
	@DynamoDBAttribute(attributeName="newsfeed_count")  
	public int getNewsfeedCount() { return newsfeed_count; }
	public void setNewsfeedCount(int newsfeed_count) { this.newsfeed_count = newsfeed_count; }
	
	@DynamoDBAttribute(attributeName=" newsfeed_ids")  
	public Set<String> getNewsfeedIds() { return  newsfeed_ids; }
	public void setNewsfeedIds(Set<String>  newsfeed_ids) { this. newsfeed_ids =  newsfeed_ids; }
	
	@DynamoDBAttribute(attributeName="since_hr")  
	public String getSinceHumanReadable() {return since_hr; } // note this should not be used. Always format and return the msfe value instead.
	public void setSinceHumanReadable(String since_hr) { this.since_hr = since_hr; }
	
	@DynamoDBAttribute(attributeName="seen_hr")  
	public String getSeenHumanReadable() {return seen_hr; } // note this should not be used. Always format and return the msfe value instead.
	public void setSeenHumanReadable(String seen_hr) { this.seen_hr = seen_hr; }
		
	@DynamoDBAttribute(attributeName="hn_karma")  
	public int getHNKarma() { return hn_karma; }
	public void setHNKarma(int hn_karma) { this.hn_karma = hn_karma; }
		
	@DynamoDBAttribute(attributeName="url_checking_mode")  
	public String getURLCheckingMode() {return url_checking_mode; }  
	public void setURLCheckingMode(String url_checking_mode) { this.url_checking_mode = url_checking_mode; }
	
	@DynamoDBAttribute(attributeName="notification_mode")  
	public String getNotificationMode() {return notification_mode; }  
	public void setNotificationMode(String notification_mode) { this.notification_mode = notification_mode; }
	
	@DynamoDBAttribute(attributeName="hn_about")  
	public String getHNAbout() {return hn_about; }  
	public void setHNAbout(String hn_about) { this.hn_about = hn_about; }
	
	@DynamoDBAttribute(attributeName="hn_topcolor")  
	public String getHNTopcolor() {return hn_topcolor; }  
	public void setHNTopcolor(String hn_topcolor) { this.hn_topcolor = hn_topcolor; }
	
	@DynamoDBAttribute(attributeName="hn_authtoken")  
	public String getHNAuthToken() {return hn_authtoken; }  
	public void setHNAuthToken(String hn_authtoken) { this.hn_authtoken = hn_authtoken; }
	
	@DynamoDBAttribute(attributeName="registered")
	@DynamoDBIndexHashKey(attributeName="registered", globalSecondaryIndexName="registered-seen-index") 
	public boolean getRegistered() {return registered; }  
	public void setRegistered(boolean registered) { this.registered = registered; }
	
	@DynamoDBAttribute(attributeName="followers")  
	public Set<String> getFollowers() { return followers; }
	public void setFollowers(Set<String> followers) { this.followers = followers; }
	
	@DynamoDBAttribute(attributeName="following")  
	public Set<String> getFollowing() { return following; }
	public void setFollowing(Set<String> following) { this.following = following; }
		
	@DynamoDBAttribute(attributeName="karma_pool")  
	public int getKarmaPool() { return karma_pool; }
	public void setKarmaPool(int karma_pool) { this.karma_pool = karma_pool; }
	
	@DynamoDBAttribute(attributeName="last_karma_pool_drain")  
	public long getLastKarmaPoolDrain() {return last_karma_pool_drain; }
	public void setLastKarmaPoolDrain(long last_karma_pool_drain) { this.last_karma_pool_drain = last_karma_pool_drain; }
	
	@DynamoDBAttribute(attributeName="karma_pool_ttl_mins")  
	public int getKarmaPoolTTLMins() { return karma_pool_ttl_mins; }
	public void setKarmaPoolTTLMins(int karma_pool_ttl_mins) { this.karma_pool_ttl_mins = karma_pool_ttl_mins; }
	
	@DynamoDBAttribute(attributeName="hide_embedded_counts")  
	public boolean getHideEmbeddedCounts() {return hide_embedded_counts; }  
	public void setHideEmbeddedCounts(boolean hide_embedded_counts) { this.hide_embedded_counts = hide_embedded_counts; }
	
	@DynamoDBAttribute(attributeName="hide_inline_follow")  
	public boolean getHideInlineFollow() {return hide_inline_follow; }  
	public void setHideInlineFollow(boolean hide_inline_follow) { this.hide_inline_follow = hide_inline_follow; }
	
	@DynamoDBAttribute(attributeName="hide_deep_reply_notifications")  
	public boolean getHideDeepReplyNotifications() {return hide_deep_reply_notifications; }  
	public void setHideDeepReplyNotifications(boolean hide_deep_reply_notifications) { this.hide_deep_reply_notifications = hide_deep_reply_notifications; }
	
	@DynamoDBAttribute(attributeName="hide_promo_links")  
	public boolean getHidePromoLinks() {return hide_promo_links; }  
	public void setHidePromoLinks(boolean hide_promo_links) { this.hide_promo_links = hide_promo_links; }
	
	@DynamoDBAttribute(attributeName="ext_version")  
	public String getExtVersion() {return ext_version; }  
	public void setExtVersion(String ext_version) { this.ext_version = ext_version; }
	
	@DynamoDBIgnore
	public boolean isValid(String inc_this_access_token)
	{
		if(inc_this_access_token == null || getThisAccessToken() == null)
			return false;
		long now = System.currentTimeMillis();
		if(getThisAccessToken().equals(inc_this_access_token) && getThisAccessTokenExpires() >= now)
			return true;
		else
			return false;
	}
	
	@DynamoDBIgnore
	public JSONObject getAsJSONObject(AmazonDynamoDBClient client, DynamoDBMapper mapper, DynamoDBMapperConfig dynamo_config)
	{
		JSONObject user_jo = new JSONObject();
		try {
			user_jo.put("screenname", getId());
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); // return msfe values formatted like this. 
			sdf.setTimeZone(TimeZone.getTimeZone("America/New_York"));
			// so we can change the formatting to whatever we want here, regardless of what is in the (meaningless) "HumanReadable" columns in the database
			if(getURLCheckingMode() == null || getURLCheckingMode().isEmpty())
				user_jo.put("url_checking_mode", "stealth");
			else
				user_jo.put("url_checking_mode", getURLCheckingMode());
			if(getNotificationMode() == null || getNotificationMode().isEmpty())
				user_jo.put("notification_mode", "newsfeed_and_notifications");
			else
				user_jo.put("notification_mode", getNotificationMode());
			user_jo.put("since", getSince());
			user_jo.put("since_hr", sdf.format(getSince()));
			if(getHNTopcolor() == null || getHNTopcolor().isEmpty())
				user_jo.put("hn_topcolor", "ff6600");
			else
				user_jo.put("hn_topcolor", getHNTopcolor());
			user_jo.put("hn_karma", getHNKarma());
			user_jo.put("hn_since", getHNSince());
			if(getKarmaPoolTTLMins() > 1440 || getKarmaPoolTTLMins() < 5)
			{
				setKarmaPoolTTLMins(10);
				mapper.save(this);
			}
			user_jo.put("karma_pool_ttl_mins", getKarmaPoolTTLMins());
			if(getFollowing() != null)
				user_jo.put("following", getFollowing());
			if(getFollowers() != null)
				user_jo.put("followers", getFollowers());
			user_jo.put("this_access_token", getThisAccessToken());
			user_jo.put("permission_level", getPermissionLevel());
			user_jo.put("seen", sdf.format(getSeen()));
			user_jo.put("notification_count", getNotificationCount());
			user_jo.put("newsfeed_count", getNewsfeedCount());
			if(getNotificationIds() != null)
				user_jo.put("notification_ids", getNotificationIds());
			if(getNewsfeedIds() != null)
				user_jo.put("newsfeed_ids", getNewsfeedIds());
			user_jo.put("hide_embedded_counts", this.getHideEmbeddedCounts());
			user_jo.put("hide_inline_follow", this.getHideInlineFollow());	
			user_jo.put("hide_deep_reply_notifications", this.getHideDeepReplyNotifications());	
			user_jo.put("hide_promo_links", this.getHidePromoLinks());
			
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return user_jo;
	}
	

	@DynamoDBIgnore
	public HashSet<NotificationItem> getNotificationItems(int minutes_ago, DynamoDBMapper mapper, DynamoDBMapperConfig dynamo_config) { 
		// set up an expression to query screename#id
		DynamoDBQueryExpression<NotificationItem> queryExpression = new DynamoDBQueryExpression<NotificationItem>()
				.withIndexName("user_id-action_msfe-index")
				.withScanIndexForward(true)
				.withConsistentRead(false);
	        
		// set the user_id part
		NotificationItem key = new NotificationItem();
		key.setUserId(getId());
		queryExpression.setHashKeyValues(key);
		
		// set the msfe range part
		if(minutes_ago > 0)
		{
			//System.out.println("Getting comment children with a valid cutoff time.");
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.MINUTE, (minutes_ago * -1));
			long msfe_cutoff = cal.getTimeInMillis();
			// set the msfe range part
			Map<String, Condition> keyConditions = new HashMap<String, Condition>();
			keyConditions.put("action_msfe",new Condition()
			.withComparisonOperator(ComparisonOperator.GT)
			.withAttributeValueList(new AttributeValue().withN(new Long(msfe_cutoff).toString())));
			queryExpression.setRangeKeyConditions(keyConditions);
		}	

		// execute
		List<NotificationItem> notificationitems = mapper.query(NotificationItem.class, queryExpression, dynamo_config);
		if(notificationitems != null && notificationitems.size() > 0)
		{	
			HashSet<NotificationItem> returnset = new HashSet<NotificationItem>();
			char c = 'X';
			for (NotificationItem notificationitem : notificationitems) {
				c = notificationitem.getId().charAt(10);
				if(!(c == '7' || c == '8')) // all but 'a user you're following did X'
					returnset.add(notificationitem);
			}
			return returnset;
		}
		else
		{
			return null;
		}
	}
	
	
	@DynamoDBIgnore
	public HashSet<NotificationItem> getNewsfeedItems(int minutes_ago, DynamoDBMapper mapper, DynamoDBMapperConfig dynamo_config) { 
		// set up an expression to query screename#id
		DynamoDBQueryExpression<NotificationItem> queryExpression = new DynamoDBQueryExpression<NotificationItem>()
				.withIndexName("user_id-action_msfe-index")
				.withScanIndexForward(true)
				.withConsistentRead(false);
	        
		// set the user_id part
		NotificationItem key = new NotificationItem();
		key.setUserId(getId());
		queryExpression.setHashKeyValues(key);
		
		// set the msfe range part
		if(minutes_ago > 0)
		{
			//System.out.println("Getting comment children with a valid cutoff time.");
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.MINUTE, (minutes_ago * -1));
			long msfe_cutoff = cal.getTimeInMillis();
			// set the msfe range part
			Map<String, Condition> keyConditions = new HashMap<String, Condition>();
			keyConditions.put("action_msfe",new Condition()
			.withComparisonOperator(ComparisonOperator.GT)
			.withAttributeValueList(new AttributeValue().withN(new Long(msfe_cutoff).toString())));
			queryExpression.setRangeKeyConditions(keyConditions);
		}	

		// execute
		List<NotificationItem> notificationitems = mapper.query(NotificationItem.class, queryExpression, dynamo_config);
		if(notificationitems != null && notificationitems.size() > 0)
		{	
			HashSet<NotificationItem> returnset = new HashSet<NotificationItem>();
			char c = 'X';
			for (NotificationItem notificationitem : notificationitems) {
				c = notificationitem.getId().charAt(10);
				if(c == '7' || c == '8')
					returnset.add(notificationitem);
			}
			return returnset;
		}
		else
		{
			return null;
		}
	}
	
	@DynamoDBIgnore
	public HashSet<HNItemItem> getHNItemsByd(int minutes_ago, DynamoDBMapper mapper, DynamoDBMapperConfig dynamo_config) { 
		// set up an expression to query screename#id
		DynamoDBQueryExpression<HNItemItem> queryExpression = new DynamoDBQueryExpression<HNItemItem>()
				.withIndexName("by-time-index")
				.withScanIndexForward(true)
				.withConsistentRead(false);
	        
		// set the user_id part
		HNItemItem key = new HNItemItem();
		key.setBy(getId());
		queryExpression.setHashKeyValues(key);
		
		// set the msfe range part
		if(minutes_ago > 0)
		{
			//System.out.println("Getting comment children with a valid cutoff time.");
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.MINUTE, (minutes_ago * -1));
			long time_cutoff = cal.getTimeInMillis() / 1000;
			// set the msfe range part
			Map<String, Condition> keyConditions = new HashMap<String, Condition>();
			keyConditions.put("time",new Condition()
			.withComparisonOperator(ComparisonOperator.GT)
			.withAttributeValueList(new AttributeValue().withN(new Long(time_cutoff).toString())));
			queryExpression.setRangeKeyConditions(keyConditions);
		}	

		// execute
		List<HNItemItem> notificationitems = mapper.query(HNItemItem.class, queryExpression, dynamo_config);
		if(notificationitems != null && notificationitems.size() > 0)
		{	
			HashSet<HNItemItem> returnset = new HashSet<HNItemItem>();
			for (HNItemItem notificationitem : notificationitems) {
				returnset.add(notificationitem);
			}
			return returnset;
		}
		else
		{
			return null;
		}
	}
	
	/*
	// this method does both notifications and newsfeed with one query rather than 2. Only useful if it's desirable that minutes_ago is the same for both.
	@DynamoDBIgnore
	public HashMap<String,HashSet<NotificationItem>> getNotificationAndNewsfeedItems(int minutes_ago, DynamoDBMapper mapper, DynamoDBMapperConfig dynamo_config) { 
		// set up an expression to query screename#id
		DynamoDBQueryExpression<NotificationItem> queryExpression = new DynamoDBQueryExpression<NotificationItem>()
				.withIndexName("user_id-action_msfe-index")
				.withScanIndexForward(true)
				.withConsistentRead(false);
	        
		// set the user_id part
		NotificationItem key = new NotificationItem();
		key.setUserId(getId());
		queryExpression.setHashKeyValues(key);
		
		// set the msfe range part
		if(minutes_ago > 0)
		{
			//System.out.println("Getting comment children with a valid cutoff time.");
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.MINUTE, (minutes_ago * -1));
			long msfe_cutoff = cal.getTimeInMillis();
			// set the msfe range part
			Map<String, Condition> keyConditions = new HashMap<String, Condition>();
			keyConditions.put("action_msfe",new Condition()
			.withComparisonOperator(ComparisonOperator.GT)
			.withAttributeValueList(new AttributeValue().withN(new Long(msfe_cutoff).toString())));
			queryExpression.setRangeKeyConditions(keyConditions);
		}	

		// execute
		List<NotificationItem> notificationitems = mapper.query(NotificationItem.class, queryExpression, dynamo_config);
		if(notificationitems != null && notificationitems.size() > 0)
		{	
			HashSet<NotificationItem> notificationset = new HashSet<NotificationItem>();
			HashSet<NotificationItem> newsfeedset = new HashSet<NotificationItem>();
			char c = 'X';
			for (NotificationItem notificationitem : notificationitems) {
				c = notificationitem.getId().charAt(10);
				if(c == '7' || c == '8')
					newsfeedset.add(notificationitem);
				else
					notificationset.add(notificationitem);
			}
			HashMap<String,HashSet<NotificationItem>> returnmap = new HashMap<String,HashSet<NotificationItem>>();
			returnmap.put("newsfeed", newsfeedset);
			returnmap.put("notifications", notificationset);
			return returnmap;
		}
		else
		{
			return null;
		}
	}*/
	
	@DynamoDBIgnore
	public int compareTo(UserItem o) // this makes more recent comments come first
	{
	    String otherscreenname = ((UserItem)o).getId();
	    int x = otherscreenname.compareTo(getId());
	    if(x >= 0) // this is to prevent equals
	    	return 1;
	    else
	    	return -1;
	}
}
