package club.hackbook;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.TimeZone;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.UUID;

import org.jsoup.Jsoup;
import org.jsoup.Connection.Response;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.SaveBehavior;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.firebase.client.DataSnapshot;

public class FirebaseChangeProcessor extends java.lang.Thread {

	private DynamoDBMapper mapper;
	private DynamoDBMapperConfig dynamo_config;
	private AmazonDynamoDBClient client;
	private DataSnapshot snapshot;
	public void initialize()
	{
	}
	
	public FirebaseChangeProcessor(DataSnapshot inc_snapshot, DynamoDBMapper inc_mapper, DynamoDBMapperConfig inc_dynamo_config, AmazonDynamoDBClient inc_client)
	{
		this.initialize();
		mapper = inc_mapper;
		dynamo_config = inc_dynamo_config;
		client = inc_client;
		snapshot = inc_snapshot;
	}
		
	@SuppressWarnings("unchecked")
	public void run()
	{
		System.out.println("=== " + super.getId() +  " Fired a FirebaseChangeProcessor thread.");
		long entry = System.currentTimeMillis();
		
		 // if it's been long enough, fire a periodic calculator thread
		 GlobalvarItem periodic_last_msfe_gvi = mapper.load(GlobalvarItem.class, "periodic_last_msfe", dynamo_config);
		 long periodic_last_msfe_long = periodic_last_msfe_gvi.getNumberValue();
		 if((entry - periodic_last_msfe_long) > 1200000) // 20 minutes
		 {
			 PeriodicCalculator pc = new PeriodicCalculator(mapper, dynamo_config, client);
			 pc.start();
		 }
		 
		 System.out.println("2Data changed " + snapshot.getChildrenCount());
		 ArrayList<String> str_value_al = null;
		 ArrayList<Integer> int_value_al = null;
		 JSONObject new_jo;
		  
		 try{
			  
			 HashMap<String, Long> score_changes = new HashMap<String,Long>(); 
			 //HashMap<String, Integer> karma_changes = new HashMap<String,Integer>(); 
			  
			 for (DataSnapshot child : snapshot.getChildren())
			 {
				  /***
				   *     _____ _____ ________  ___ _____ 
				   *    |_   _|_   _|  ___|  \/  |/  ___|
				   *      | |   | | | |__ | .  . |\ `--. 
				   *      | |   | | |  __|| |\/| | `--. \
				   *     _| |_  | | | |___| |  | |/\__/ /
				   *     \___/  \_/ \____/\_|  |_/\____/ 
				   *                                     
				   */
				  if(child.getName().equals("items"))
				  {
					  int_value_al = child.getValue(ArrayList.class);
					  System.out.println(child.getName() + " " + int_value_al.toString());
					  HNItemItem hnii = null;
					  String result = null;
					  Iterator<Integer> it = int_value_al.iterator();
					  Integer item = null;
					  while(it.hasNext())
					  {
						  item = it.next();
						  if(item != null) // strangely, this item in the array CAN be null. FIXME do this below too?
						  {  
							 try{
								  Response r = Jsoup
										 .connect("https://hacker-news.firebaseio.com/v0/item/" + item.intValue()  + ".json")
										 .userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36")
										 .ignoreContentType(true).execute();
								  result = r.body();
								  hnii = mapper.load(HNItemItem.class, item.intValue(), dynamo_config);
								  if(hnii == null)
								  {
									  /*** NEW ITEM ***/
									  System.out.println("item: " + item.intValue() + " " + "does not exist. Creating.");
									  hnii = createItemFromHNAPIResult(result, true);
									  if(hnii != null)
										  mapper.save(hnii);
								  }
								  else
								  {
									  /*** hnii is already an EXISTING ITEM ***/
									  HashSet<Long> oldkids = null;
									  HashSet<Long> newkids = null;
									  if(hnii.getKids() == null)
										  oldkids = new HashSet<Long>(); 
									  else
										  oldkids = (HashSet<Long>)hnii.getKids();
									  
									  HNItemItem new_hnii = createItemFromHNAPIResult(result, false);
									  if(new_hnii != null) // creation successful
									  {
										  if(new_hnii.getKids() != null) // if newkids is null, there's obviously nothing to do
										  {
											  newkids = (HashSet<Long>)new_hnii.getKids();
												  
											  Iterator<Long> it2 = oldkids.iterator();
											  while(it2.hasNext())
											  {
												  newkids.remove(it2.next());
											  }
												  
											  Iterator<Long> newminusoldit = newkids.iterator();
											  Long currentnewkid = 0L;
											  while(newminusoldit.hasNext())
											  {
												  currentnewkid = newminusoldit.next();
												  System.out.print("kid: " + currentnewkid + " ");
												  HNItemItem hnitemitem = mapper.load(HNItemItem.class, currentnewkid, dynamo_config);
												  if(hnitemitem == null)
												  {
													  System.out.print("adding! ");
													  Response r2 = Jsoup
															  .connect("https://hacker-news.firebaseio.com/v0/item/" + currentnewkid  + ".json")
															  .userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36")
															  .ignoreContentType(true).execute();
													  String kid_result = r2.body();
													  hnitemitem = createItemFromHNAPIResult(kid_result, true);
													  if(hnitemitem != null)
														  mapper.save(hnitemitem);
												  }
											  }
										  }
										  //mapper.delete(hnii); // replace old item 
										  if(new_hnii.getKids() != null && new_hnii.getKids().size() == 0)
											  new_hnii.setKids(null);
										  mapper.save(new_hnii, new DynamoDBMapperConfig(SaveBehavior.CLOBBER));
										  
										  if(hnii.getType().equals("story") && hnii.getScore() != new_hnii.getScore())
										  {
											  long old_score = hnii.getScore();
											  long new_score = new_hnii.getScore();
											  if(old_score != new_score)
											  {
												  if(new_score > old_score)
													  System.out.println("New score: +" + (new_score - old_score));
												  else
													  System.out.println("New score: -" + (old_score - new_score));
												  
												  UserItem author = mapper.load(UserItem.class, hnii.getBy(), dynamo_config);
												  if(author == null)
												  {
													  System.out.print(" Author of this item is NOT in the database.");
												  }
												  else
												  {
													  System.out.print(" Author of this item IS in the database");
													  if(author.getRegistered())
													  {
														  System.out.print(" and registered!");
														  // add this score change to the parent's notification feed
														  if(new_score > old_score)
														  {
															  createNotificationItem(author, "3", hnii.getId(), hnii.getTime()*1000, null, 0);		 // feedable event 3, a story you wrote was upvoted
															  if(score_changes.containsKey(author.getId()))
																  score_changes.put(author.getId(), (new_score-old_score)+score_changes.get(author.getId()));
														  }
														  else
														  {
															  createNotificationItem(author, "4", hnii.getId(), hnii.getTime()*1000, null, 0);	 // feedable event 4, a story you wrote was downvoted
															  if(score_changes.containsKey(author.getId()))
																  score_changes.put(author.getId(), (old_score-new_score)+score_changes.get(author.getId()));
														  }
													  }
													  else
													  {
														  System.out.print(" but NOT a registered user. Ignore.");
													  }
												  }
												  System.out.println();
											  }
										  }
									  }
								  }
							  }
							  catch(IOException ioe)
							  {
								  System.err.println("IOException getting item " + item.intValue() + ", but execution should continue.");
							  }
						  }
					  }
				  }
				  /***
				   *    ____________ ___________ _____ _      _____ _____ 
				   *    | ___ \ ___ \  _  |  ___|_   _| |    |  ___/  ___|
				   *    | |_/ / |_/ / | | | |_    | | | |    | |__ \ `--. 
				   *    |  __/|    /| | | |  _|   | | | |    |  __| `--. \
				   *    | |   | |\ \\ \_/ / |    _| |_| |____| |___/\__/ /
				   *    \_|   \_| \_|\___/\_|    \___/\_____/\____/\____/ 
				   *                                                      
				   *                                                      
				   */
				  else if(child.getName().equals("profiles"))
				  {	  
					  str_value_al = child.getValue(ArrayList.class);
					  System.out.println(child.getName() + " " + str_value_al.toString());
					  UserItem useritem = null;
					  String result = null;
					  Iterator<String> it = str_value_al.iterator();
					  String screenname = null;
					  while(it.hasNext())
					  {
						 screenname = it.next();	
						 result = Jsoup
								 .connect("https://hacker-news.firebaseio.com/v0/user/" + screenname  + ".json")
								 .userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36")
								 .ignoreContentType(true).execute().body();
						 useritem = mapper.load(UserItem.class, screenname, dynamo_config);
						 if(useritem == null)
						 {
							 System.out.println("Creating " + screenname + ". ");
							 useritem = createUserFromHNAPIResult(result);
							 if(useritem != null)
								 mapper.save(useritem);
						 }
						 else
						 {
							  System.out.println("Updating " + screenname + ". ");
							  if(!(result == null || result.isEmpty())) // 
							  {
								  try{
									  new_jo = new JSONObject(result);
									  boolean saveuseritem = false;
									 
									  int new_karma = new_jo.getInt("karma");
									  int old_karma = useritem.getHNKarma();
									  if(old_karma != new_karma)
									  {
										  int ttl = useritem.getKarmaPoolTTLMins();
										  if(ttl > 1440 || ttl < 5) // one day to five minutes is the acceptable range. If not, reset it to 15 mins.
										  {
											  ttl = 15;
											  useritem.setKarmaPoolTTLMins(ttl);
										  }
										  
										  if(useritem.getLastKarmaPoolDrain() < System.currentTimeMillis() - (ttl*60000)) // it's been more than ttl minutes
										  {
											  int change = useritem.getKarmaPool() + (new_karma - old_karma);
											  System.out.println("old_karma=" + old_karma + " new_karma=" + new_karma + " reporting change " + change + " if the user is registered.");
											  if(change != 0 && useritem.getRegistered()) // this change in karma for this firebase increment (30 sec) + the total change in the karma pool (10 mins) cancel each other out.
											  {											  // create notification only if registered
												  if(change > 0)
													  createNotificationItem(useritem, "1", 0L, System.currentTimeMillis(), null, change); // feedable event 1, positive karma change
												  else if(change < 0)				
													  createNotificationItem(useritem, "2", 0L, System.currentTimeMillis(), null, change); // feedable event 2, negative karma change
											  }
											  // regardless, empty the pool and set new timestamp
											  useritem.setHNKarma(new_karma);
											  useritem.setKarmaPool(0);
											  useritem.setLastKarmaPoolDrain(System.currentTimeMillis());
										  }
										  else
										  {
											  System.out.println("Updating karma pool existing=" + useritem.getKarmaPool() + " change=" + (new_karma - old_karma));
											  useritem.setKarmaPool(useritem.getKarmaPool() + (new_karma - old_karma));
										  }
										  saveuseritem = true;
									  }
									  else // the user's karma as reported by HN API right now is exactly the same as what we've got on file. Do nothing.
									  {
										  
									  }	 
									  
									  // keep track of "about" changes? I guess. Why not.
									  String old_about = useritem.getHNAbout();
									  String new_about = "";
									  if(new_jo.has("about"))
										  new_about = new_jo.getString("about");
									  if(old_about == null || new_about == null) // one or both was null
									  {
										  if(old_about == null && new_about == null)
										  { 
											  // both null, no change, do nothing
										  }
										  else if(old_about == null && new_about != null)
										  {
											  System.out.println("ABOUT: old_about was null. new_about is not. Saving.");
											  useritem.setHNAbout(new_about);
											  saveuseritem = true; 
										  }
										  else if(old_about != null && new_about == null)
										  {
											  System.out.println("ABOUT: old_about was not null. new_about is. Saving.");
											  useritem.setHNAbout(null);
											  saveuseritem = true; 
										  }
									  }
									  else
									  {
										  if(!old_about.equals(new_about))
										  {
											  System.out.println("ABOUT: about string changed. Saving.");
											  useritem.setHNAbout(new_about);
											  saveuseritem = true; 
										  }
										  // else neither null, both equal, no change. do nothing.
									  }
									
									  
									  if(saveuseritem)
										  mapper.save(useritem, new DynamoDBMapperConfig(SaveBehavior.CLOBBER));
									  
									  /*
									  // no reason to keep track of this user's submitted items because any new items will appear in the ITEMS block above.
									  JSONArray old_submitted_ja = new JSONArray();
									  if(old_jo.has("submitted"))
										  old_submitted_ja = old_jo.getJSONArray("submitted");
									  List<String> old_submitted_list = new ArrayList<String>();
									  for(int i = 0; i < old_submitted_ja.length(); i++){
										  old_submitted_list.add(old_submitted_ja.getString(i));
									  }
									  
									  JSONArray new_submitted_ja = new JSONArray();
									  if(new_jo.has("submitted"))
										  new_submitted_ja = new_jo.getJSONArray("submitted");
									  List<String> new_submitted_list = new ArrayList<String>();
									  for(int i = 0; i < new_submitted_ja.length(); i++){
										  new_submitted_list.add(new_submitted_ja.getString(i));
									  }
									  
									  Iterator<String> it2 = old_submitted_list.iterator();
									  while(it2.hasNext())
									  {
										  new_submitted_list.remove(it2.next());
									  }
									  
									  Iterator<String> newminusoldit = new_submitted_list.iterator();
									  while(newminusoldit.hasNext())
									  {
										  System.out.println("Found new submitted: " + newminusoldit.next());
									  }								  
									   */
									  
								  }
								  catch(JSONException jsone)
								  {
									  jsone.printStackTrace();
								  }
							  }
						  }
					  }
				  }
				  else 
				  {
					  System.err.println("child.getName() was something other than \"items\" or \"profiles\"");
				  }
			  }
		  } catch (IOException e) {
			  // TODO Auto-generated catch block
			  e.printStackTrace();
		  }
		
		long exit = System.currentTimeMillis();
		System.out.println("=== " + super.getId() +  " FirebaseChangeProcessor Done. elapsed=" + ((exit - entry)/1000) + "s");
		return;
	}
	

    private HNItemItem createItemFromHNAPIResult(String unchecked_result, boolean processFeeds)
    {
    	if(unchecked_result == null || unchecked_result.isEmpty())
    	{
    		System.err.println("createItemFromHNAPIResult(): Error trying to create new item in DB: result string from HN api was null or empty");
    		return null;
    	}
    	try{
    		HNItemItem hnii = null;
    		JSONObject new_jo = new JSONObject(unchecked_result);
    		// these are the required fields (as far as we're concerned)
    		// without them, we can't even make sense of what to do with it
    		if(new_jo.has("id") && new_jo.has("by") && new_jo.has("time") && new_jo.has("type")) 
    		{
				  /*** THESE FIELDS MUST MATCH HNItemItem EXACTLY ***/
				  
    			/*
    private long id; 
	private String by;
	private long time;
	private String type;
	private boolean dead;
	private boolean deleted;
	private long parent;
	private long score;
	private Set<Long> kids;
	private String url;
    			 */
    			
				  hnii = new HNItemItem();
				  hnii.setId(new_jo.getLong("id"));
				  // new fields
				  hnii.setBy(new_jo.getString("by"));
				  hnii.setTime(new_jo.getLong("time"));
				  hnii.setType(new_jo.getString("type"));
				 
				  if(new_jo.has("dead") && new_jo.getBoolean("dead") == true)
					  hnii.setDead(true);
				  else
					  hnii.setDead(false);
				  if(new_jo.has("deleted") && new_jo.getBoolean("deleted") == true)
					  hnii.setDeleted(true);
				  else
					  hnii.setDeleted(false);
				  
				  if(new_jo.has("parent"))
					  hnii.setParent(new_jo.getLong("parent"));
				  
				  if(new_jo.has("score"))
					  hnii.setScore(new_jo.getLong("score"));
				  
				  if(new_jo.has("kids"))
				  {
					  HashSet<Long> kids_ts = new HashSet<Long>();
					  JSONArray ja = new_jo.getJSONArray("kids");
					  if(ja != null && ja.length() > 0)
					  {	  
						  int x = 0;
						  while(x < ja.length())
						  {
							  kids_ts.add(ja.getLong(x));
							  x++;
						  }
						  if(kids_ts.size() == ja.length()) // if the number of items has changed for some reason, just skip bc something has messed up
						  {
							  System.out.println("createItemFromHNAPIResult(): setting kids=" + kids_ts.size());
							  hnii.setKids(kids_ts);
						  }
					  }
					  else
						  hnii.setKids(null);
				  }
				  
				  if(new_jo.has("url"))
					  hnii.setURL(new_jo.getString("url"));
				  
				  long now = System.currentTimeMillis();
				  if(processFeeds && ((hnii.getTime()*1000) > (now - 86400000))) // make sure this is within the past day before sending any notifications
				  {
					  //System.out.println("** Processing new HNItemItem for feeds. item time=" + (hnii.getTime()*1000) + " > cutoff=" + (now-86400000));
					  if(hnii.getType().equals("comment") && !hnii.getDead() && !hnii.getDeleted())
					  {
						  processNewCommentForFeeds(hnii);
					  }
					  else if(hnii.getType().equals("story") && !hnii.getDead() && !hnii.getDeleted())
					  {
						  processNewStoryForFeeds(hnii);
					  }
					  else
					  {
						  // deleted, dead or something other than comment/story (poll, for instance)
					  }
				  }
				  else
				  {
					  System.out.println("createItemFromHNAPIResult(): Too old. item time=" + (hnii.getTime()*1000) + " < cutoff=" + (now-86400000));
				  }
				  
				  return hnii;
    		}
    		else
    		{
    			System.err.println("createItemFromHNAPIResult(): Error trying to create new item in DB: missing required id, by, time or type values");
    			return null;
    		}
		  }
		  catch(JSONException jsone)
		  {
			  System.err.println("createItemFromHNAPIResult(): Error trying to create new item in DB: result string was not valid JSON.");
			  return null;
		  }
    }
    
    private void processNewCommentForFeeds(HNItemItem hnii) throws JSONException
    {
    	/***
    	 *    ____________ _____ _____  _____ _____ _____   _____ ________  ______  ___ _____ _   _ _____  ______ ___________  ______ _____ ___________  _____ 
    	 *    | ___ \ ___ \  _  /  __ \|  ___/  ___/  ___| /  __ \  _  |  \/  ||  \/  ||  ___| \ | |_   _| |  ___|  _  | ___ \ |  ___|  ___|  ___|  _  \/  ___|
    	 *    | |_/ / |_/ / | | | /  \/| |__ \ `--.\ `--.  | /  \/ | | | .  . || .  . || |__ |  \| | | |   | |_  | | | | |_/ / | |_  | |__ | |__ | | | |\ `--. 
    	 *    |  __/|    /| | | | |    |  __| `--. \`--. \ | |   | | | | |\/| || |\/| ||  __|| . ` | | |   |  _| | | | |    /  |  _| |  __||  __|| | | | `--. \
    	 *    | |   | |\ \\ \_/ / \__/\| |___/\__/ /\__/ / | \__/\ \_/ / |  | || |  | || |___| |\  | | |   | |   \ \_/ / |\ \  | |   | |___| |___| |/ / /\__/ /
    	 *    \_|   \_| \_|\___/ \____/\____/\____/\____/   \____/\___/\_|  |_/\_|  |_/\____/\_| \_/ \_/   \_|    \___/\_| \_| \_|   \____/\____/|___/  \____/ 
    	 *                                                                                                                                                     
    	 *                                                                                                                                                     
    	 */
    	
    	  HashSet<String> already_notified_users = new HashSet<String>(); 
		  if(hnii == null) // if the hnii input is invalid, return.
		  {
			  System.out.println("processNewCommentForFeeds(hnii): Cannot proceed because hnii is null. Returning.");
			  return;
		  }
		  
		  String triggerer = hnii.getBy();
		  HNItemItem current_hnii = hnii;
		  UserItem current_author;
		  int parentlevel = 0; // 1=parents, 2=grandparents, etc
		  while(current_hnii.getParent() != 0L)
		  {
			  current_hnii = mapper.load(HNItemItem.class, current_hnii.getParent(), dynamo_config); 
			  if(current_hnii == null)
				  break;
			  parentlevel++;
			  current_author = mapper.load(UserItem.class, current_hnii.getBy(), dynamo_config);
			  if(current_author == null)
			  {
				  System.out.println("processNewCommentForFeeds(hnii): author of parent (" + current_hnii.getBy() + ") is not NOT in the database, skipping.");
			  }
			  else
			  {
				  System.out.println("processNewCommentForFeeds(hnii): author of parent (" + current_hnii.getBy() + ") IS in the database, checking if registered and not already notified...");
				  if(!current_author.getId().equals(triggerer) // don't notify a person of deep-replies to himself 
						  && current_author.getRegistered() 
						  && !already_notified_users.contains(current_author.getId())) // a user could have multiple parents above this incoming comment. Don't notify them multiple times.
				  {
					  System.out.println("processNewCommentForFeeds(hnii): author of parent (" + current_hnii.getBy() + ") IS in the database, registered and not already notified. Creating notification.");
					  if(parentlevel == 1)
					  {
						  if(current_hnii.getType().equals("comment"))
						  {
							  System.out.println("processNewCommentForFeeds(hnii): Adding replied-to-comment notification.");
							  createNotificationItem(current_author, "5", hnii.getId(), hnii.getTime()*1000, hnii.getBy(), 0); // feedable event 5, a comment current_author wrote was replied to
							  already_notified_users.add(current_author.getId());
						  }
						  else if(current_hnii.getType().equals("story"))
						  {
							  System.out.println("processNewCommentForFeeds(hnii): Adding commented-on-story notification.");
							  createNotificationItem(current_author, "6", hnii.getId(), hnii.getTime()*1000, hnii.getBy(), 0); // feedable event 6, a story current_author wrote was replied to
							  already_notified_users.add(current_author.getId());
						  }
						  // else, polls etc
					  }
					  else // parentlevel > 1
					  {
						  if(current_hnii.getType().equals("comment"))
						  {
							  System.out.println("processNewCommentForFeeds(hnii): Adding deep replied-to-comment notification.");
							  createNotificationItem(current_author, "9", hnii.getId(), hnii.getTime()*1000, hnii.getBy(), 0); // feedable event 9, a comment current_author wrote was deep-replied to
							  already_notified_users.add(current_author.getId());
						  }
						  else if(current_hnii.getType().equals("story")) // this is where functionality to notify story posters on deep replies would be added
						  {
							  System.out.println("processNewCommentForFeeds(hnii): Skipping deep replied-to-story notification.");
						  }
						  // else, polls etc
					  }
				  }
				  else
				  {
					  System.out.println("processNewCommentForFeeds(hnii): author of parent (" + current_hnii.getBy() + ") IS in the database but NOT registered. Skipping notifications.");
				  }
			  }
		  }
		  
		  // check for followers of this comment's \"by\" and alert them
		  UserItem author = mapper.load(UserItem.class, hnii.getBy(), dynamo_config);
		  if(author == null)
		  {
			  System.out.println("processNewCommentForFeeds(hnii): Author of this comment (" + hnii.getBy() + ") is NOT in the database. Skipping notifications.");
		  }
		  else
		  {
			  System.out.println("processNewCommentForFeeds(hnii): Author of this comment (" + hnii.getBy() + ") IS in the database. Checking to see if they have any followers.");
			  HashSet<String> followers = (HashSet<String>) author.getFollowers();
			  if(followers == null)
			  {
				  System.out.println("processNewCommentForFeeds(hnii): No followers.");
			  }
			  else if(followers.isEmpty())
			  {
				  System.out.println("processNewCommentForFeeds(hnii): No followers. (getFollwers() was Empty (not null) and we saved it to null just now.)");
				  author.setFollowers(null);
				  mapper.save(author);
			  }
			  else
			  { 
				  System.out.println("processNewCommentForFeeds(hnii): author (" + hnii.getBy() + ") has followers.");
				  Iterator<String> followers_it = followers.iterator();
				  String currentfollower = "";
				  UserItem followeruseritem = null;
				  while(followers_it.hasNext())
				  {
					  currentfollower = followers_it.next();
					  followeruseritem = mapper.load(UserItem.class, currentfollower, dynamo_config);
					  if(followeruseritem != null && followeruseritem.getRegistered()) // if a user is following this commenter, they should be registered, but I guess this check is fine.						
					  {  
						  System.out.println("processNewCommentForFeeds(hnii): Found a valid, registered follower (" + followeruseritem.getId() + ") and creating notification.");
						  if(!already_notified_users.contains(followeruseritem.getId())) // only send notification if the user hasn't been alerted in the reply-checking block above.
							  createNotificationItem(followeruseritem, "8", hnii.getId(), hnii.getTime()*1000, author.getId(), 0); // feedable event 8, a user you're following commented
					  }
				  }
			  }
		  }
    }
    
    private void processNewStoryForFeeds(HNItemItem hnii) throws JSONException
    {
    	/***
    	 *    ____________ _____ _____  _____ _____ _____   _____ _____ _____________   __ ______ ___________  ______ _____ ___________  _____ 
    	 *    | ___ \ ___ \  _  /  __ \|  ___/  ___/  ___| /  ___|_   _|  _  | ___ \ \ / / |  ___|  _  | ___ \ |  ___|  ___|  ___|  _  \/  ___|
    	 *    | |_/ / |_/ / | | | /  \/| |__ \ `--.\ `--.  \ `--.  | | | | | | |_/ /\ V /  | |_  | | | | |_/ / | |_  | |__ | |__ | | | |\ `--. 
    	 *    |  __/|    /| | | | |    |  __| `--. \`--. \  `--. \ | | | | | |    /  \ /   |  _| | | | |    /  |  _| |  __||  __|| | | | `--. \
    	 *    | |   | |\ \\ \_/ / \__/\| |___/\__/ /\__/ / /\__/ / | | \ \_/ / |\ \  | |   | |   \ \_/ / |\ \  | |   | |___| |___| |/ / /\__/ /
    	 *    \_|   \_| \_|\___/ \____/\____/\____/\____/  \____/  \_/  \___/\_| \_| \_/   \_|    \___/\_| \_| \_|   \____/\____/|___/  \____/ 
    	 *                                                                                                                                     
    	 *                                                                                                                                     
    	 */
    	
    	  if(hnii == null || !hnii.getType().equals("story") || hnii.getDead() || hnii.getDeleted())
    	  {
    		  System.out.println("processNewStoryForFeeds(hnii): hnii was null, or not a \"story\" or dead or deleted. Skipping processing of feeds.");
    		  return;
    	  }

    	  if(hnii.getScore() > 1) // this is a brand new story, but already has an upvote
			  System.out.println("processNewStoryForFeeds(hnii): NOTE: This is a BRAND NEW STORY (with url) THAT ALREADY HAS AN UPVOTE. Proceeding.");
		  
		  // check for followers of this new story's \"by\" and alert them
		  UserItem author = mapper.load(UserItem.class, hnii.getBy(), dynamo_config);
		  if(author == null)
		  {
			  System.out.println("processNewStoryForFeeds(hnii): Author of this story is NOT in the database. Skipping processing of feeds.");
		  }
		  else
		  {
			  System.out.println("processNewStoryForFeeds(hnii): Author of this story is in the database. See if they have any followers.");
			  HashSet<String> followers = (HashSet<String>) author.getFollowers();
			  if(followers == null)
			  {
				  System.out.println("processNewStoryForFeeds(hnii): No followers.");
			  }
			  else if(followers.isEmpty())
			  {
				  System.out.println("processNewStoryForFeeds(hnii): No followers. (getFollwers() was Empty (not null) and we saved it to null just now.)");
				  author.setFollowers(null);
				  mapper.save(author);
			  }
			  else
			  {
				  System.out.println("processNewStoryForFeeds(hnii): author has followers.");
				  Iterator<String> followers_it = followers.iterator();
				  String currentfollower = "";
				  UserItem followeruseritem = null;
				  while(followers_it.hasNext())
				  {
					  currentfollower = followers_it.next();
					  followeruseritem = mapper.load(UserItem.class, currentfollower, dynamo_config);
					  if(followeruseritem != null && followeruseritem.getRegistered()) // if a user is following this poster, they should be registered, but I guess this check is fine.		
					  {  
						  System.out.println("processNewStoryForFeeds(hnii): Found a valid, registered follower (" + followeruseritem + ") and creating notification.");
						  createNotificationItem(followeruseritem, "7", hnii.getId(), hnii.getTime()*1000, author.getId(), 0); // feedable event 7, a user you're following posted a story with a URL
					  }
				  }
			  }
		  }
    }
    /***
     * 
     
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
	private boolean hide_hn_new;
	private boolean hide_hn_threads;
	private boolean hide_hn_comments;
	private boolean hide_hn_show;
	private boolean hide_hn_ask;
	private boolean hide_hn_jobs;
	private boolean hide_hn_submit;
	private boolean hide_hn_feed;
	private boolean hide_hn_notifications;
     */
    
    private UserItem createUserFromHNAPIResult(String result)
    {
    	if(result == null || result.isEmpty())
    		return null;
    	try 
    	{ 
    		UserItem useritem = new UserItem();
    		JSONObject profile_jo = new JSONObject(result);
    		useritem.setId(profile_jo.getString("id"));
    		useritem.setHNKarma(profile_jo.getInt("karma"));
    		useritem.setHNSince(profile_jo.getLong("created"));
    		useritem.setId(profile_jo.getString("id"));
    		useritem.setRegistered(false);
    		useritem.setURLCheckingMode("stealth");
    		useritem.setKarmaPoolTTLMins(15);
    		useritem.setHideEmbeddedCounts(true);
    		if(profile_jo.has("about"))
    			useritem.setHNAbout(profile_jo.getString("about"));
    		else
    			useritem.setHNAbout("");
    		return useritem;
    	} catch (JSONException e) {
    		e.printStackTrace();
    		return null;
    	}
    }   

    
    private boolean createNotificationItem(UserItem useritem, String type, long hn_target_id, long action_time, String triggerer, int karma_change)
    {
    	if(!useritem.getRegistered()) // never create notification items for users that aren't registered.
    		return false;
    	
    	long now = System.currentTimeMillis();
    	String now_str = Global.fromDecimalToBase62(7,action_time);
    	Random generator = new Random(); 
		int r = generator.nextInt(238327); // this will produce numbers that can be represented by 3 base62 digits
    	String randompart_str = Global.fromDecimalToBase62(3,r);
		String notification_id = now_str + randompart_str + type; 
		
		NotificationItem ni = new NotificationItem();
		ni.setId(notification_id);
		ni.setActionMSFE(action_time);
		ni.setMSFE(now);
		ni.setUserId(useritem.getId());
		ni.setType(type);
		if(type.equals("1") || type.equals("2"))
		{
			ni.setHNRootId(0L);
			ni.setHNRootStoryOrPollId(0L);
			ni.setHNRootCommentId(0L);
			ni.setHNTargetId(0L);
			ni.setTriggerer(null);
			ni.setKarmaChange(karma_change);
		}
		else if(type.equals("3") || type.equals("4") || type.equals("7")) // a story you wrote was upvoted, downvoted or a user you're following wrote a story
		{
			ni.setHNTargetId(hn_target_id);
			ni.setHNRootId(hn_target_id);
			ni.setHNRootStoryOrPollId(hn_target_id);
			ni.setHNRootCommentId(0L);
			ni.setTriggerer(triggerer);
		}
		else if(type.equals("5") || type.equals("6") || type.equals("8") || type.equals("9")) // a comment you wrote was replied to(5) or deep-replied to(9) or story you wrote was commented on(6) or a user you're following commented(8). Crawl back to root.
		{
			ni.setHNTargetId(hn_target_id);
			System.out.println("Found an item we need to step back on to find root.");
			HashMap<String,Long> roots = Global.findRootStoryAndComment(hn_target_id);
			if(roots == null)
				return false; // couldn't find root, so bail
			ni.setHNRootId(roots.get("story_or_poll"));
			ni.setHNRootStoryOrPollId(roots.get("story_or_poll"));
			ni.setHNRootCommentId(roots.get("comment")); // for #6, this should always be the same as hn_target_id
			ni.setTriggerer(triggerer);
		}
		mapper.save(ni);
		
		if(type.equals("7") || type.equals("8"))// the two newsfeed types
		{
			
			TreeSet<String> newsfeedset = new TreeSet<String>();
	    	if(useritem.getNewsfeedIds() != null)
	    		newsfeedset.addAll(useritem.getNewsfeedIds());
	    	newsfeedset.add(notification_id);
	    	while(newsfeedset.size() > Global.NEWSFEED_SIZE_LIMIT)
	    		newsfeedset.remove(newsfeedset.first());
	    	useritem.setNewsfeedIds(newsfeedset);
	    	if(useritem.getNotificationMode() != null && useritem.getNotificationMode().equals("newsfeed_and_notifications")) // only do this if they want news feed notifications
	    		useritem.setNewsfeedCount(useritem.getNewsfeedCount()+1);
	    	mapper.save(useritem);
		}
		else // everything else, i.e. the notification types
		{	
			TreeSet<String> notificationset = new TreeSet<String>();
	    	if(useritem.getNotificationIds() != null)
	    		notificationset.addAll(useritem.getNotificationIds());
	    	notificationset.add(notification_id);
	    	while(notificationset.size() > Global.NOTIFICATIONS_SIZE_LIMIT)
	    		notificationset.remove(notificationset.first());
	    	useritem.setNotificationIds(notificationset);
	    	useritem.setNotificationCount(useritem.getNotificationCount()+1);
	    	mapper.save(useritem);
		}
    	return true;
    }
	
}