package club.hackbook;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.SaveBehavior;
import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;
import com.firebase.client.ValueEventListener;
 
public class FirebaseListener implements ServletContextListener {
 
    private static ExecutorService executor;
    private AWSCredentials credentials;
	private AmazonDynamoDBClient client;
	private DynamoDBMapper mapper;
	private DynamoDBMapperConfig dynamo_config;
    private Firebase myFirebaseRef = null;
    private String myId = ""; 
    GlobalvarItem firebase_owner_id_gvi = null;
    GlobalvarItem firebase_last_msfe_gvi = null;
    Doer doer = null;
    
    @SuppressWarnings("unchecked")
    @Override
    public void contextInitialized(ServletContextEvent cs) {
    	try {
    		//System.out.println("Initializing DynamoDBMapper from Endpoint.init()");
    		credentials = new PropertiesCredentials(getClass().getClassLoader().getResourceAsStream("AwsCredentials.properties"));
    		client = new AmazonDynamoDBClient(credentials);
    		client.setRegion(Region.getRegion(Regions.US_EAST_1)); 
    		mapper = new DynamoDBMapper(client);
    		dynamo_config = new DynamoDBMapperConfig(DynamoDBMapperConfig.ConsistentReads.EVENTUAL);
    		doer = new Doer();
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    	myId = UUID.randomUUID().toString().replaceAll("-", "");
    	myFirebaseRef = new Firebase("https://hacker-news.firebaseio.com/v0/updates");
    	
    	createExecutor();
    	cs.getServletContext().log("Executor service started !");
    	myFirebaseRef.addValueEventListener(new ValueEventListener() {

 			  @Override
 			  public void onDataChange(DataSnapshot snapshot) {
 				  
 				 // If I own the firebase lock OR the firebase lock is more than 5 minutes old, take it over.
 				 
 				 
 				 long now = System.currentTimeMillis();
 				 //boolean ihavethepower = true;
 				 
 				 boolean ihavethepower = false;
 				 firebase_owner_id_gvi = mapper.load(GlobalvarItem.class, "firebase_owner_id", dynamo_config); // we can assume this is never null
 				 firebase_last_msfe_gvi = mapper.load(GlobalvarItem.class, "firebase_last_msfe", dynamo_config); // we can assume this is never null
				 if(!firebase_owner_id_gvi.getStringValue().equals(myId))
				 {
					 if(firebase_last_msfe_gvi.getNumberValue() > System.currentTimeMillis()-300000) 
					 {
						 System.out.println("*** I do NOT have the power! *** myId=" + myId + " owner=" + firebase_owner_id_gvi.getStringValue() + " firebase_last_msfe is " + (System.currentTimeMillis()-firebase_last_msfe_gvi.getNumberValue()) + " old.");
						 ihavethepower = false;
					 }
					 else
					 {
						 System.out.println("*** I DO have the power! (takeover) *** myId=" + myId + " owner=" + firebase_owner_id_gvi.getStringValue() + " firebase_last_msfe is " + (System.currentTimeMillis()-firebase_last_msfe_gvi.getNumberValue()) + " old.");
						 firebase_owner_id_gvi.setStringValue(myId);
						 firebase_last_msfe_gvi.setNumberValue(now);
						 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
						 sdf.setTimeZone(TimeZone.getTimeZone("America/New_York"));
						 firebase_last_msfe_gvi.setStringValue(sdf.format(now));
						 mapper.save(firebase_owner_id_gvi);
						 mapper.save(firebase_last_msfe_gvi);
						 ihavethepower = true;
					 }
				 }
				 else // if(firebase_owner_id_gvi.getStringValue().equals(myId))
				 {
					 System.out.println("*** I DO have the power! (keep) *** myId=" + myId + " owner=" + firebase_owner_id_gvi.getStringValue() + " firebase_last_msfe is " + (System.currentTimeMillis()-firebase_last_msfe_gvi.getNumberValue()) + " old.");
					 firebase_owner_id_gvi.setStringValue(myId);
					 firebase_last_msfe_gvi.setNumberValue(System.currentTimeMillis());
					 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
					 sdf.setTimeZone(TimeZone.getTimeZone("America/New_York"));
					 firebase_last_msfe_gvi.setStringValue(sdf.format(now));
					 mapper.save(firebase_owner_id_gvi);
					 mapper.save(firebase_last_msfe_gvi);
					 ihavethepower = true;
				 }
 				  
				 if(ihavethepower)
				 { 
					 // if it's been long enough, fire a periodic calculator thread
					 GlobalvarItem periodic_last_msfe_gvi = mapper.load(GlobalvarItem.class, "periodic_last_msfe", dynamo_config);
					 long periodic_last_msfe_long = periodic_last_msfe_gvi.getNumberValue();
					 if((now - periodic_last_msfe_long) > 1200000) // 20 minutes
					 {
						 PeriodicCalculator pc = new PeriodicCalculator(mapper, dynamo_config, client);
						 pc.start();
					 }
					 
					 System.out.println("Data changed " + snapshot.getChildrenCount());
					 ArrayList<String> str_value_al = null;
					 ArrayList<Integer> int_value_al = null;
					 JSONObject old_jo;
					 JSONObject new_jo;
					  
					 try{
						  
						 HashMap<String, Long> score_changes = new HashMap<String,Long>(); 
						 HashMap<String, Integer> karma_changes = new HashMap<String,Integer>(); 
						  
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
												  hnii = createNewHNItemFromAPIResult(result, true);
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
												  
												  HNItemItem new_hnii = createNewHNItemFromAPIResult(result, false);
												  if(new_hnii != null && new_hnii.getKids() != null) // if newkids is null, there's obviously nothing to do
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
														  System.out.println("Found new kid: " + currentnewkid);
														  HNItemItem hnitemitem = mapper.load(HNItemItem.class, currentnewkid, dynamo_config);
														  if(hnitemitem == null)
														  {
															  System.out.println("new kid " + currentnewkid + " is not in the db... adding");
															  Response r2 = Jsoup
																	  .connect("https://hacker-news.firebaseio.com/v0/item/" + currentnewkid  + ".json")
																	  .userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36")
																	  .ignoreContentType(true).execute();
															  String kid_result = r2.body();
															  hnitemitem = createNewHNItemFromAPIResult(kid_result, true);
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
																	  createNotificationItem(author, "3", hnii.getId(), null, 0);		 // feedable event 3, a story you wrote was upvoted
																	  if(score_changes.containsKey(author.getId()))
																		  score_changes.put(author.getId(), (new_score-old_score)+score_changes.get(author.getId()));
																  }
																  else
																  {
																	  createNotificationItem(author, "4", hnii.getId(), null, 0);	 // feedable event 4, a story you wrote was downvoted
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
										  System.out.println("screenname: " + screenname + " does not exist. Creating.");
										  createNewUser(result);
		 							 }
		 							 else
		 							 {
										  System.out.println("screenname: " + screenname + " already exists. Updating.");
										  String old_profile_str = useritem.getHNProfile();
										  if(old_profile_str == null) // the user existed in the database, but had no profile, for some reason. Set it and then skip the rest.
										  {
											  System.err.println("User existed in the database, but had no hn_profile for some reason. Setting it and moving on. If this continues, fix.");
											  useritem.setHNProfile(result);
											  mapper.save(useritem);
										  }
										  else
										  {	  
											  try{
												  old_jo = new JSONObject(useritem.getHNProfile());
												  new_jo = new JSONObject(result);
												//  System.out.println("----------------------------------------------");
												 // System.out.println("old=" + old_jo);
												 // System.out.println("new=" + new_jo);
												  
												  useritem.setHNProfile(result);
												  mapper.save(useritem);
												  int old_karma = old_jo.getInt("karma");
												  int new_karma = new_jo.getInt("karma");
												  if(old_karma != new_karma)
												  {
													  // feedable items 1-2?
													  if(new_karma > old_karma)
													  {
														  System.out.println("New karma score: +" + (new_karma - old_karma));
														  if(karma_changes.containsKey(useritem.getId()))
															  karma_changes.put(useritem.getId(), (new_karma-old_karma)+karma_changes.get(useritem.getId()));
														  createNotificationItem(useritem, "1", 0L, null, (new_karma-old_karma));
													  }
													  else
													  {
														  System.out.println("New karma score: -" + (old_karma - new_karma));
														  if(karma_changes.containsKey(useritem.getId()))
															  karma_changes.put(useritem.getId(), (new_karma-old_karma)+karma_changes.get(useritem.getId()));
														  createNotificationItem(useritem, "2", 0L, null, (old_karma-new_karma));
													  }
												  }
												  String old_about = "";
												  String new_about = "";
												  if(old_jo.has("about"))
													  old_about = old_jo.getString("about");
												  if(new_jo.has("about"))
													  new_about = new_jo.getString("about");
												  if(!old_about.equals(new_about))
													  System.out.println("New about message: " + new_about);

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
											  }
											  catch(JSONException jsone)
											  {
												  jsone.printStackTrace();
											  }
										  }
										 // System.out.println("----------------------------------------------");
									  }
		 						  }
		 					  }
		 					  else 
		 					  {
		 						  System.err.println("child.getName() was something other than \"items\" or \"profiles\"");
		 					  }
		 				  }
						  
						 /* System.out.println("******** Story score changes after parsing items: ***********");
						  for (Map.Entry<String, Integer> entry : user_scorechanges_after_items.entrySet()) 
						  { 
							  System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue()); 
						  }
						  
						  System.out.println("******** Karma changes after parsing profiles: ***********");
						  for (Map.Entry<String, Integer> entry : user_karmachanges_after_profiles.entrySet()) 
						  { 
							  System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue()); 
						  }
						  
						  //System.out.println("******** Calculating master changes: ***********");
						  HashMap<String, Integer> masterkarmachanges = new HashMap<String,Integer>();
						  String currentkarmakey = "";
						  int currentkarmavalue = 0;
						  String currentstoryscorekey = "";
						  int currentstoryscorevalue = 0;
						  boolean found = false;
						  for (Map.Entry<String, Integer> entry : user_karmachanges_after_profiles.entrySet()) 
						  { 
							  //System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
							  currentkarmakey = entry.getKey();
							  currentkarmavalue = entry.getValue();
							  found = false;
							  for (Map.Entry<String, Integer> entry2 : user_scorechanges_after_items.entrySet()) 
							  { 
								 // System.out.println("\tKey = " + entry2.getKey() + ", Value = " + entry2.getValue()); 
								  currentstoryscorekey = entry2.getKey();
								  currentstoryscorevalue = entry2.getValue();
								  if(currentkarmakey.equals(currentstoryscorekey))
								  {
									  if(currentkarmavalue != currentstoryscorevalue) // there is a discrepancy between karma change and story score changes. This means score changed elsewhere (comments).
										  masterkarmachanges.put(currentkarmakey, currentkarmavalue - currentstoryscorevalue);
									  found = true; // the two values are equal, so no additional karma change outside of the storychanges.
								  }
							  }
							  if(found == false)
							  {
								  masterkarmachanges.put(currentkarmakey, currentkarmavalue);
							  }
						  }
						  
						  System.out.println("******** Master karma changes (excluding stories): ***********");
						  // feedable events 1 & 2
						  for (Map.Entry<String, Integer> entry : masterkarmachanges.entrySet()) 
						  { 
							  System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue()); 
						  }*/
						  
					  } catch (IOException e) {
						  // TODO Auto-generated catch block
						  e.printStackTrace();
					  }
 			  	}
 			  }

 			  @Override public void onCancelled(FirebaseError error) {
 				  System.out.println("onCancelled called");
 			  }
 			});
    }
 
    @Override
    public void contextDestroyed(ServletContextEvent cs) {
 
     executor.shutdown();
 
    cs.getServletContext().log("Executor service shutdown !");
    }
 
    public static synchronized void submitTask(Runnable runnable) {
        if (executor == null) {
            createExecutor();
        }
        executor.submit(runnable);
    }
 
    public static synchronized Future<String> submitTask(Callable callable) {
        if (executor == null) {
            createExecutor();
        }
        return executor.submit(callable);
    }
 
    static void  createExecutor() {
        executor = new ThreadPoolExecutor(
                1,
                3,
                100L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
 
    }
    
    private HNItemItem createNewHNItemFromAPIResult(String unchecked_result, boolean processFeeds)
    {
    	if(unchecked_result == null || unchecked_result.isEmpty())
    	{
    		System.err.println("Error trying to create new item in DB: result string from HN api was null or empty");
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
							  System.out.println("createNewHNItemFromAPIResult setting kids=" + kids_ts.size());
							  hnii.setKids(kids_ts);
						  }
					  }
					  else
						  hnii.setKids(null);
				  }
				  
				  if(new_jo.has("url"))
					  hnii.setURL(new_jo.getString("url"));
				  
				  mapper.save(hnii);
				  
				  long now = System.currentTimeMillis();
				  if(processFeeds && ((hnii.getTime()*1000) > (now - 86400000))) 
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
					  //System.out.println("** NOT Processing new HNItemItem for feeds. item time=" + (hnii.getTime()*1000) + " < cutoff=" + (now-86400000));
				  }
				  
				 
				  return hnii;
    		}
    		else
    		{
    			System.err.println("Error trying to create new item in DB: missing required id, by, time or type values");
    			return null;
    		}
		  }
		  catch(JSONException jsone)
		  {
			  System.err.println("Error trying to create new item in DB: result string was not valid JSON.");
			  return null;
		  }
    }
    
    private void processNewCommentForFeeds(HNItemItem hnii) throws JSONException
    {
    	 /***
		   *     _   _  _____ _    _   _____ _____ ________  ___           _____ ________  ______  ___ _____ _   _ _____ 
		   *    | \ | ||  ___| |  | | |_   _|_   _|  ___|  \/  |          /  __ \  _  |  \/  ||  \/  ||  ___| \ | |_   _|
		   *    |  \| || |__ | |  | |   | |   | | | |__ | .  . |  ______  | /  \/ | | | .  . || .  . || |__ |  \| | | |  
		   *    | . ` ||  __|| |/\| |   | |   | | |  __|| |\/| | |______| | |   | | | | |\/| || |\/| ||  __|| . ` | | |  
		   *    | |\  || |___\  /\  /  _| |_  | | | |___| |  | |          | \__/\ \_/ / |  | || |  | || |___| |\  | | |  
		   *    \_| \_/\____/ \/  \/   \___/  \_/ \____/\_|  |_/           \____/\___/\_|  |_/\_|  |_/\____/\_| \_/ \_/  
		   *                                                                                                             
		   */
    	
		  if(hnii == null)
		  {
			  System.out.print(" which is NOT on file in the db.");
		  }
		  else
		  {
			  System.out.print("Processing comment for notification feeds. parent=" + hnii.getParent() + " which is ");
			  HNItemItem parent_hnii = mapper.load(HNItemItem.class, hnii.getParent(), dynamo_config);
			  if(parent_hnii != null)
			  {
				  System.out.print(" in the database and whose by=" + parent_hnii.getBy() + " is ");
				  UserItem parent_author = mapper.load(UserItem.class, parent_hnii.getBy(), dynamo_config);
				  if(parent_author == null)
				  {
					  System.out.print("NOT in the database.");
				  }
				  else
				  {
					  System.out.print("in the database ");
					  if(parent_author.getRegistered())
					  {
						  System.out.print("and registered!");
						  if(hnii.getType().equals("comment"))
							  createNotificationItem(parent_author, "5", hnii.getId(), hnii.getBy(), 0); // feedable event 5, a comment parent_author wrote was replied to
						  else if(hnii.getType().equals("story"))
							  createNotificationItem(parent_author, "6", hnii.getId(), hnii.getBy(), 0); // feedable event 6, a story parent_author wrote was replied to
					  }
					  else
					  {
						  System.out.print("but NOT a registered user.");
					  }
				  }
			  }
			  else
			  {
				  System.out.print("NOT in the database. Moving on.");
			  }
		  }
		  
		  // check for followers of this comment's \"by\" and alert them
		  UserItem author = mapper.load(UserItem.class, hnii.getBy(), dynamo_config);
		  if(author == null)
		  {
			  System.out.print("Author of this comment is NOT in the database.");
		  }
		  else
		  {
			  System.out.print("Author of this comment IS in the database");
			  HashSet<String> followers = (HashSet<String>) author.getFollowers();
			  if(followers == null)
			  {
				  System.out.print(" but getFollowers() was null.");
			  }
			  else if(followers.isEmpty())
			  {
				  System.out.print(" but getFollowers() was empty.");
			  }
			  else
			  { 
				  System.out.print(" and getFollowers() was not empty.");
				  Iterator<String> followers_it = followers.iterator();
				  String currentfollower = "";
				  UserItem followeruseritem = null;
				  while(followers_it.hasNext())
				  {
					  currentfollower = followers_it.next();
					  followeruseritem = mapper.load(UserItem.class, currentfollower, dynamo_config);
					  if(followeruseritem != null && followeruseritem.getRegistered()) // if a user is following this commenter, they should be registered, but I guess this check is fine.						
					  {  
						  createNotificationItem(followeruseritem, "8", hnii.getId(), author.getId(), 0); // feedable event 8, a user you're following commented
					  }
				  }
				  System.out.print("]");
			  }
		  }
		  System.out.println();
    }
    
    private void processNewStoryForFeeds(HNItemItem hnii) throws JSONException
    {
    	/***
		   *     _   _  _____ _    _   _____ _____ ________  ___           _____ _____ _____________   __
		   *    | \ | ||  ___| |  | | |_   _|_   _|  ___|  \/  |          /  ___|_   _|  _  | ___ \ \ / /
		   *    |  \| || |__ | |  | |   | |   | | | |__ | .  . |  ______  \ `--.  | | | | | | |_/ /\ V / 
		   *    | . ` ||  __|| |/\| |   | |   | | |  __|| |\/| | |______|  `--. \ | | | | | |    /  \ /  
		   *    | |\  || |___\  /\  /  _| |_  | | | |___| |  | |          /\__/ / | | \ \_/ / |\ \  | |  
		   *    \_| \_/\____/ \/  \/   \___/  \_/ \____/\_|  |_/          \____/  \_/  \___/\_| \_| \_/  
		   *                                                                                             
		   *                                                                                             
		   */
    	  if(!hnii.getType().equals("story") || hnii.getDead() || hnii.getDeleted() || hnii.getURL() == null)
    	  	  return;

    	  if(hnii.getScore() > 1) // this is a brand new story, but already has an upvote
			  System.out.println("BRAND NEW STORY (with url) THAT ALREADY HAS AN UPVOTE");
		  
		  // check for followers of this new story's \"by\" and alert them
		  UserItem author = mapper.load(UserItem.class, hnii.getBy(), dynamo_config);
		  if(author == null)
		  {
			  System.out.print(" Author of this story is NOT in the database.");
		  }
		  else
		  {
			  System.out.print(" Author of this story IS in the database");
			  HashSet<String> followers = (HashSet<String>) author.getFollowers();
			  if(followers == null)
			  {
				  System.out.print(" but getFollowers() was null.");
			  }
			  else if(followers.isEmpty())
			  {
				  System.out.print(" but getFollowers() was empty.");
			  }
			  else
			  { 
				  System.out.print(" and getFollowers() was not empty. [");
				  Iterator<String> followers_it = followers.iterator();
				  String currentfollower = "";
				  UserItem followeruseritem = null;
				  while(followers_it.hasNext())
				  {
					  currentfollower = followers_it.next();
					  followeruseritem = mapper.load(UserItem.class, currentfollower, dynamo_config);
					  if(followeruseritem != null && followeruseritem.getRegistered()) // if a user is following this poster, they should be registered, but I guess this check is fine.		
					  {  
						  createNotificationItem(followeruseritem, "7", hnii.getId(), author.getId(), 0); // feedable event 7, a user you're following posted a story with a URL
						  System.out.print(followeruseritem.getId() + ", ");
					  }
				  }
				  System.out.print("]");
			  }
		  }
		  System.out.println();
			
    }
    
    private boolean createNotificationItem(UserItem useritem, String type, long hn_target_id, String triggerer, int karma_change)
    {
    	if(!useritem.getRegistered()) // never create notification items for users that aren't registered.
    		return false;
    	long now = System.currentTimeMillis();
    	String now_str = Global.fromDecimalToBase62(7,now);
    	Random generator = new Random(); 
		int r = generator.nextInt(238327); // this will produce numbers that can be represented by 3 base62 digits
    	String randompart_str = Global.fromDecimalToBase62(3,r);
		String notification_id = now_str + randompart_str + type; 
		
		NotificationItem ni = new NotificationItem();
		ni.setId(notification_id);
		ni.setMSFE(now);
		ni.setUserId(useritem.getId());
		ni.setType(type);
		if(type.equals("1") || type.equals("2"))
		{
			ni.setHNRootId(0L);																	
			ni.setHNTargetId(0L);
			ni.setTriggerer(null);
			ni.setKarmaChange(karma_change);
		}
		else if(type.equals("3") || type.equals("4") || type.equals("6") || type.equals("7")) // a story you wrote was upvoted, downvoted or commented on...
		{
			ni.setHNTargetId(hn_target_id);
			ni.setHNRootId(hn_target_id);																	 //  or a user you're following posted a story (+/-) url
			ni.setTriggerer(triggerer);
		}
		else if(type.equals("5") || type.equals("8")) // a comment you wrote was commented on or a user you're following commented. Crawl back to root.
		{
			ni.setHNTargetId(hn_target_id);
			System.out.println("Found an item we need to step back on to find root.");
			long root = Global.findRootItem(hn_target_id);
			if(root == -1)
				return false; // couldn't find root, so bail
			else
				ni.setHNRootId(root);
			ni.setTriggerer(triggerer);
		}
		mapper.save(ni);
		
		if(type.equals("7") || type.equals("8"))
		{
			
			TreeSet<String> newsfeedset = new TreeSet<String>();
	    	if(useritem.getNewsfeedIds() != null)
	    		newsfeedset.addAll(useritem.getNewsfeedIds());
	    	newsfeedset.add(notification_id);
	    	while(newsfeedset.size() > Global.NEWSFEED_SIZE_LIMIT)
	    		newsfeedset.remove(newsfeedset.first());
	    	useritem.setNewsfeedIds(newsfeedset);
	    	useritem.setNewsfeedCount(useritem.getNewsfeedCount()+1);
	    	mapper.save(useritem);
		}
		else
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
    
    private boolean createNewUser(String result)
    {
    	try { 
    		UserItem useritem = new UserItem();
    		useritem.setHNProfile(result);
    		JSONObject profile_jo = new JSONObject(result);
    		useritem.setHNKarma(profile_jo.getInt("karma"));
    		useritem.setLastKarmaCheck(System.currentTimeMillis());
    		useritem.setHNSince(profile_jo.getLong("created"));
    		useritem.setId(profile_jo.getString("id"));
    		useritem.setRegistered(false);
    		useritem.setURLCheckingMode("stealth");
    		if(profile_jo.has("about"))
    			useritem.setHNAbout(profile_jo.getString("about"));
    		else
    			useritem.setHNAbout("");
    		mapper.save(useritem);
    		return true;
    	} catch (JSONException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    		return false;
    	}
    }
    
}