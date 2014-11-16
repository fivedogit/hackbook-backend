package club.hackbook;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.Calendar;
import java.util.TreeSet;
import java.util.UUID;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jsoup.Jsoup;
import org.jsoup.Connection.Response;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class Endpoint extends HttpServlet {

	// static variables:
	private static final long serialVersionUID = 1L;

	private AWSCredentials credentials;
	private AmazonDynamoDBClient client;
	private DynamoDBMapper mapper;
	private DynamoDBMapperConfig dynamo_config;
	private boolean devel = false;

	public void init(ServletConfig servlet_config) throws ServletException {
		try {
			// System.out.println("Initializing DynamoDBMapper from Endpoint.init()");
			credentials = new PropertiesCredentials(getClass().getClassLoader()
					.getResourceAsStream("AwsCredentials.properties"));
			client = new AmazonDynamoDBClient(credentials);
			client.setRegion(Region.getRegion(Regions.US_EAST_1));
			mapper = new DynamoDBMapper(client);
			dynamo_config = new DynamoDBMapperConfig(
					DynamoDBMapperConfig.ConsistentReads.EVENTUAL);
		} catch (IOException e) {
			e.printStackTrace();
		}

		super.init(servlet_config);
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		response.setContentType("application/json; charset=UTF-8;");
		response.setHeader("Access-Control-Allow-Origin", "*"); // FIXME
		PrintWriter out = response.getWriter();
		JSONObject jsonresponse = new JSONObject();
		try {
			jsonresponse.put("response_status", "error");
			jsonresponse.put("message", "This endpoint doesn't speak POST.");
			out.println(jsonresponse);
		} catch (JSONException jsone) {
			out.println("{ \"response_status\": \"error\", \"message\": \"JSONException caught in Endpoint POST\"}");
			System.err.println("endpoint: JSONException thrown in doPost(). "
					+ jsone.getMessage());
		}
		return;
	}

	// error codes:
	// 0000 = delete everything
	// 0001 = delete social token

	// this method queries for all 4 equal permutations of the incoming url
	private HashSet<HNItemItem> getAllHNItemsFromURL(String url_str,
			int minutes_ago) {
		HashSet<HNItemItem> tempset = null;
		HashSet<HNItemItem> combinedset = new HashSet<HNItemItem>();
		tempset = getHNItemsFromURL(url_str, minutes_ago);
		if (tempset != null) {
			combinedset.addAll(tempset); // as-is
			tempset = null;
		}

		// ideally, we'd try with/without slash on all the permutations below.
		// But that turns 4 lookups into 8, so let's just do the as-is +/- slash
		if (!url_str.endsWith("/")) {
			tempset = getHNItemsFromURL(url_str + "/", minutes_ago); // as-is +
																		// trailing
																		// "/"
			if (tempset != null) {
				combinedset.addAll(tempset);
				tempset = null;
			}
		} else {
			String url_str_minus_trailing_slash = url_str.substring(0,
					url_str.length() - 1);
			tempset = getHNItemsFromURL(url_str_minus_trailing_slash,
					minutes_ago); // as-is minus trailing "/"
			if (tempset != null) {
				combinedset.addAll(tempset);
				tempset = null;
			}
		}
		if (url_str.startsWith("https://")) {
			if (url_str.startsWith("https://www.")) // https & www.
			{
				tempset = getHNItemsFromURL("http://" + url_str.substring(12),
						minutes_ago); // try http && !www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
				tempset = getHNItemsFromURL("https://" + url_str.substring(12),
						minutes_ago); // try https && !www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
				tempset = getHNItemsFromURL(
						"http://www." + url_str.substring(12), minutes_ago); // try
																				// http
																				// &&
																				// www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
			} else if (!url_str.startsWith("https://www.")) // https & !www.
			{
				tempset = getHNItemsFromURL("http://" + url_str.substring(8),
						minutes_ago); // try http && !www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
				tempset = getHNItemsFromURL(
						"http://www." + url_str.substring(8), minutes_ago); // try
																			// http
																			// &&
																			// www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
				tempset = getHNItemsFromURL(
						"https://www." + url_str.substring(8), minutes_ago); // try
																				// https
																				// &&
																				// www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
			}
		} else if (url_str.startsWith("http://")) {
			if (url_str.startsWith("http://www.")) // http & www.
			{
				tempset = getHNItemsFromURL("http://" + url_str.substring(11),
						minutes_ago); // try http && !www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
				tempset = getHNItemsFromURL("https://" + url_str.substring(11),
						minutes_ago); // try https && !www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
				tempset = getHNItemsFromURL(
						"https://www." + url_str.substring(11), minutes_ago); // try
																				// https
																				// &&
																				// www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
			} else if (!url_str.startsWith("http://www.")) // http & !www.
			{
				tempset = getHNItemsFromURL(
						"http://www." + url_str.substring(7), minutes_ago); // try
																			// http
																			// &&
																			// www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
				tempset = getHNItemsFromURL(
						"https://www." + url_str.substring(7), minutes_ago); // try
																				// https
																				// &&
																				// www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
				tempset = getHNItemsFromURL("https://" + url_str.substring(7),
						minutes_ago); // try https && !www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
			}
		}
		return combinedset;
	}

	private HashSet<HNItemItem> getHNItemsFromURL(String url_str,
			int minutes_ago) {
		DynamoDBQueryExpression<HNItemItem> queryExpression = new DynamoDBQueryExpression<HNItemItem>()
				.withIndexName("url-time-index").withScanIndexForward(true)
				.withConsistentRead(false);

		// set the user_id part
		HNItemItem key = new HNItemItem();
		key.setURL(url_str);
		queryExpression.setHashKeyValues(key);

		// set the time range part
		if (minutes_ago > 0) {
			// System.out.println("Getting comment children with a valid cutoff time.");
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.MINUTE, (minutes_ago * -1));
			long msfe_cutoff = cal.getTimeInMillis();
			// set the msfe range part
			Map<String, Condition> keyConditions = new HashMap<String, Condition>();
			keyConditions.put(
					"time",
					new Condition().withComparisonOperator(
							ComparisonOperator.GT).withAttributeValueList(
							new AttributeValue().withN(new Long(msfe_cutoff)
									.toString())));
			queryExpression.setRangeKeyConditions(keyConditions);
		}

		// execute
		List<HNItemItem> items = mapper.query(HNItemItem.class,
				queryExpression, dynamo_config);
		if (items != null && items.size() > 0) {
			HashSet<HNItemItem> returnset = new HashSet<HNItemItem>();
			for (HNItemItem item : items) {
				returnset.add(item);
			}
			return returnset;
		} else {
			return null;
		}
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		//System.out.println("endpoint.doGet(): entering...");
		response.setContentType("application/json; charset=UTF-8;");
		response.setHeader("Access-Control-Allow-Origin","*"); //FIXME
		PrintWriter out = response.getWriter();
		JSONObject jsonresponse = new JSONObject();
		long timestamp_at_entry = System.currentTimeMillis();
		try 
		{
			String method = request.getParameter("method");
			if(!request.isSecure() && !(devel == true && request.getRemoteAddr().equals("127.0.0.1")))
			{
				jsonresponse.put("message", "This API endpoint must be communicated with securely.");
				jsonresponse.put("response_status", "error");
			}
			else if(method == null)
			{
				jsonresponse.put("message", "Method not specified. This should probably produce HTML output reference information at some point.");
				jsonresponse.put("response_status", "error");
			}
			else
			{
				/***
				 *     _   _ _____ _   _         ___  _   _ _____ _   _  ___  ___ _____ _____ _   _ ___________  _____ 
				 *    | \ | |  _  | \ | |       / _ \| | | |_   _| | | | |  \/  ||  ___|_   _| | | |  _  |  _  \/  ___|
				 *    |  \| | | | |  \| |______/ /_\ \ | | | | | | |_| | | .  . || |__   | | | |_| | | | | | | |\ `--. 
				 *    | . ` | | | | . ` |______|  _  | | | | | | |  _  | | |\/| ||  __|  | | |  _  | | | | | | | `--. \
				 *    | |\  \ \_/ / |\  |      | | | | |_| | | | | | | | | |  | || |___  | | | | | \ \_/ / |/ / /\__/ /
				 *    \_| \_/\___/\_| \_/      \_| |_/\___/  \_/ \_| |_/ \_|  |_/\____/  \_/ \_| |_/\___/|___/  \____/ 
				 */
				if(method.equals("searchForHNItem"))
				{
					String url_str = request.getParameter("url");
					if(url_str != null && !url_str.isEmpty())
					{
						HashSet<HNItemItem> hnitems = getAllHNItemsFromURL(url_str, 0);
						HNItemItem hnii = null;
						if(hnitems == null)
							hnii = null;
						else if(hnitems.size() == 1)
							hnii = hnitems.iterator().next();
						else if(hnitems.size() > 1)
						{
							System.out.println("There are multiple items matching this URL. Selecting the one with the highest score.");
							Iterator<HNItemItem> it = hnitems.iterator();
							long max = 0; 
							HNItemItem current = null;
							while(it.hasNext())
							{
								current = it.next();
								if(current.getScore() > max)
								{
									hnii = current;
									max = current.getScore();
								}
							}
						}
												
						if(hnii != null)
						{
							jsonresponse.put("response_status", "success");
							jsonresponse.put("objectID", hnii.getId());
						}
						else
						{
							jsonresponse.put("response_status", "success");
							jsonresponse.put("objectID", "-1");
						}
					}
					else
					{
						jsonresponse.put("response_status", "error");
						jsonresponse.put("message", "Invalid \"url\" parameter.");	
					}
				}
				else if(method.equals("getHNAuthToken")) // user has just chosen to log in with HN. Generate auth token for this screenname, save it, return it.
				{
					String screenname = request.getParameter("screenname");
					if(screenname == null || screenname.isEmpty())
					{
						jsonresponse.put("message", "Screenname was null or empty.");
						jsonresponse.put("response_status", "error");
					}
					else
					{
						UserItem useritem = mapper.load(UserItem.class, screenname, dynamo_config);
						if(useritem == null)
						{
							useritem = new UserItem();
							useritem.setRegistered(false);
						}
						useritem.setId(screenname);
						String uuid = UUID.randomUUID().toString().replaceAll("-","");
						useritem.setHNAuthToken(uuid);
						mapper.save(useritem);
						jsonresponse.put("response_status", "success");
						jsonresponse.put("token", uuid);
					}
				}
				else if(method.equals("verifyHNUser")) // Using the generated auth token above, user has changed their "about" page to include the token. Verify it independently.
				{										// This should probably be triggered by FirebaseListener for optimal performance.
					String screenname = request.getParameter("screenname");
					if(screenname == null || screenname.isEmpty())
					{
						jsonresponse.put("message", "Screenname was null or empty.");
						jsonresponse.put("response_status", "error");
					}
					else
					{
						UserItem useritem = mapper.load(UserItem.class, screenname, dynamo_config);
						if(useritem == null)
						{
							jsonresponse.put("message", "No user by that screenname was found in the database.");
							jsonresponse.put("response_status", "error");
						}
						else
						{	
							String stored_uuid = useritem.getHNAuthToken();
							int x = 0;
							String result = "";
							String about = "";
							String checked_uuid = "";
							int bi = 0;
							int ei = 0;
							int limit = 7;
							String hn_karma_str = "0";
							String hn_since_str = "0";
							JSONObject hn_user_jo = null;
							while(x < limit)
							{
								try
								{
									result = Jsoup
											 .connect("https://hacker-news.firebaseio.com/v0/user/" + screenname  + ".json")
											 .userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36")
											 .ignoreContentType(true).execute().body();
									
									//System.out.println("Endpoint.verifyHNUser():" + result);
									hn_user_jo = new JSONObject(result);
									about = hn_user_jo.getString("about");
									bi = about.indexOf("BEGIN|");
									if(bi != -1)                                   // entering here means the loop WILL break 1 of 3 ways: No |ENDTOKEN, match or no match.
									{
										ei = about.indexOf("|END");
										if(ei == -1)
										{
											jsonresponse.put("response_status", "error");
											jsonresponse.put("message", "Found \"BEGIN|\" but not \"|END\"");
											break;
										}
										else
										{
											checked_uuid = about.substring(bi + 6, ei);
											if(checked_uuid.equals(stored_uuid))
											{	
												SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
												sdf.setTimeZone(TimeZone.getTimeZone("America/New_York"));
												
												String uuid_str = UUID.randomUUID().toString().replaceAll("-","");
												Calendar cal = Calendar.getInstance();
												long now = cal.getTimeInMillis();
												cal.add(Calendar.YEAR, 1);
												long future = cal.getTimeInMillis();
												if(!useritem.getRegistered()) // if user is not yet registered, populate default values
												{
													useritem = new UserItem();
													//useritem.setNotificationIds();
													useritem.setNotificationCount(0);
													useritem.setOnDislike("button");
													useritem.setOnLike("button");
													useritem.setOnReply("button");
													useritem.setPermissionLevel("user");
													useritem.setId(screenname);
													useritem.setSince(now);
													useritem.setSinceHumanReadable(sdf.format(now));
													useritem.setRegistered(true);
													useritem.setURLCheckingMode("stealth");
												}
												useritem.setLastIPAddress(request.getRemoteAddr());
												useritem.setSeen(now);
												useritem.setSeenHumanReadable(sdf.format(now));
												useritem.setThisAccessToken(uuid_str);
												useritem.setThisAccessTokenExpires(future);
												useritem.setHNAuthToken(null);
												
												if(hn_user_jo.has("karma")) 
												{	
													hn_karma_str = hn_user_jo.getString("karma");
													if(Global.isWholeNumeric(hn_karma_str))
														useritem.setHNKarma(Integer.parseInt(hn_karma_str));
													else
														useritem.setHNKarma(0); // if "karma" is somehow not a whole integer, set to 0
												}
												else
													useritem.setHNKarma(0); // if "karma" is somehow missing, set to 0
												useritem.setLastKarmaCheck(now);

												if(hn_user_jo.has("created")) 
												{	
													hn_since_str = hn_user_jo.getString("created");
													if(Global.isWholeNumeric(hn_since_str))
														useritem.setHNSince(Integer.parseInt(hn_since_str));
													else
														useritem.setHNSince(0); // if "karma" is somehow not a whole integer, set to 0
												}
												else
													useritem.setHNSince(0); // if "karma" is somehow missing, set to 0
												
												mapper.save(useritem);
												
												//System.out.println("Endpoint.loginWithGoogleOrShowRegistration() user already registered, logging in");
												jsonresponse.put("response_status", "success");
												jsonresponse.put("verified", true);
												jsonresponse.put("this_access_token", uuid_str);
												jsonresponse.put("screenname", useritem.getId());
												break;
											}
											else
											{
												System.out.println("Loop " + x + ", Found BEGIN| and |END, but the string didn't match the DB. Trying again in 5 seconds.");
												try {
													java.lang.Thread.sleep(5000);
												} catch (InterruptedException e) {
													// TODO Auto-generated catch block
													e.printStackTrace();
												}
												x++;
											}
										}
									}
									else
									{
										System.out.println("Loop " + x + ", Did not find BEGIN| or |END. Trying again in 5 seconds.");
										try {
											java.lang.Thread.sleep(5000);
										} catch (InterruptedException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
										x++;
									}
								}
								catch(IOException ioe)
								{
									System.err.println("IOException attempting to verifyHNUser. Ignore and continue.");
								}
							}
							if(x == limit)
							{
								System.out.println("Checked " + limit + " times and failed. Returning response_status = error.");
								jsonresponse.put("response_status", "error");
								jsonresponse.put("message", "Checked " + limit + " times and didn't find \"BEGIN|\"");
							}
						}
					}
				}
				else if(method.equals("getMostFollowedUsers"))
				{
					GlobalvarItem gvi = mapper.load(GlobalvarItem.class, "most_followed_users", dynamo_config);
					if(gvi != null)
					{
						jsonresponse.put("response_status", "success");
						jsonresponse.put("most_followed_users", new JSONArray(gvi.getStringValue()));
					}
					else
					{
						jsonresponse.put("response_status", "error");
						jsonresponse.put("message", "Couldn't get most_followed_users value from DB.");
					}
				}
				else if(method.equals("getRandomUsers"))
				{
					GlobalvarItem gvi = mapper.load(GlobalvarItem.class, "random_users", dynamo_config);
					if(gvi != null)
					{
						jsonresponse.put("response_status", "success");
						jsonresponse.put("random_users", new JSONArray(gvi.getStringValue()));
					}
					else
					{
						jsonresponse.put("response_status", "error");
						jsonresponse.put("message", "Couldn't get random_users value from DB.");
					}
				}
				 /***
				  *    ___  ___ _____ _____ _   _ ___________  _____  ______ _____ _____     _   _ _____ ___________    ___  _   _ _____ _   _ 
				  *    |  \/  ||  ___|_   _| | | |  _  |  _  \/  ___| | ___ \  ___|  _  |   | | | /  ___|  ___| ___ \  / _ \| | | |_   _| | | |
				  *    | .  . || |__   | | | |_| | | | | | | |\ `--.  | |_/ / |__ | | | |   | | | \ `--.| |__ | |_/ / / /_\ \ | | | | | | |_| |
				  *    | |\/| ||  __|  | | |  _  | | | | | | | `--. \ |    /|  __|| | | |   | | | |`--. \  __||    /  |  _  | | | | | | |  _  |
				  *    | |  | || |___  | | | | | \ \_/ / |/ / /\__/ / | |\ \| |___\ \/' /_  | |_| /\__/ / |___| |\ \  | | | | |_| | | | | | | |
				  *    \_|  |_/\____/  \_/ \_| |_/\___/|___/  \____/  \_| \_\____/ \_/\_(_)  \___/\____/\____/\_| \_| \_| |_/\___/  \_/ \_| |_/
				  *                                                                                                                            
				  *                                                                                                                            
				  */
				 else if (method.equals("getUserSelf") || method.equals("setUserPreference") ||
						 method.equals("followUser") || method.equals("unfollowUser") ||
						 method.equals("resetNotificationCount") || method.equals("removeItemFromNotificationIds") ||  method.equals("getNotificationItem") ||
						 method.equals("resetNewsfeedCount")
						 // || method.equals("noteItemLikeOrDislike") || method.equals("haveILikedThisItem") || method.equals("haveIDislikedThisItem")
						 )
				 {
					 // for all of these methods, check email/this_access_token. Weak check first (to avoid database hits). Then check database.
					 String screenname = request.getParameter("screenname"); // the requester's email
					 String this_access_token = request.getParameter("this_access_token"); // the requester's auth
					 if(!(screenname == null || screenname.isEmpty()) && !(this_access_token == null || this_access_token.isEmpty())) 
					 {
						// both weren't null or empty
						// if only email is null or empty respond with code "0000" to clear out the malformed credentials
						if(!(screenname == null || screenname.isEmpty()))
						{
							// otherwise, continue to user retrieval
							UserItem useritem = mapper.load(UserItem.class, screenname, dynamo_config);
							if(useritem != null)
							{	
								if(useritem.isValid(this_access_token)) 
								{	
									if (method.equals("getUserSelf")) // I think this might be redundant (or maybe the one below is)
									{
										JSONObject user_jo = null;
										long now = System.currentTimeMillis();
										
										boolean something_needs_updating = false;
										if(now - useritem.getSeen() > 600000) // if it's been > 10 mins since last "seen" update, update it
										{
											something_needs_updating = true;
											useritem.setSeen(now);
											SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
											sdf.setTimeZone(TimeZone.getTimeZone("America/New_York"));
											useritem.setSeenHumanReadable(sdf.format(timestamp_at_entry));
											useritem.setLastIPAddress(request.getRemoteAddr()); // necessary for spam and contest fraud prevention
										}
										if(now - useritem.getLastKarmaCheck() > 3600000) // if it's been an hour since last karma check, check it again
										{
											something_needs_updating = true;
											try{
												String result = Jsoup
													 .connect("https://hacker-news.firebaseio.com/v0/user/" + screenname  + ".json")
													 .userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36")
													 .ignoreContentType(true).execute().body();
												JSONObject hn_user_jo = new JSONObject(result);
												if(hn_user_jo.has("karma"))
													useritem.setHNKarma(hn_user_jo.getInt("karma"));
											}
											catch(IOException ioe){
												//
											}
											catch(JSONException jsone){
												//
											}
											useritem.setLastKarmaCheck(now); // whether or not the above worked, act like it did to prevent repetitive karma checks
										}
										if(something_needs_updating)
											mapper.save(useritem);
										
										boolean get_this_access_token = true; 
										boolean get_notification_ids = true; 
										boolean get_email_preferences = true; 
										boolean get_notification_count = true;
										boolean get_seen = true;
										user_jo = useritem.getAsJSONObject(get_this_access_token, get_notification_ids, get_email_preferences, get_notification_count, get_seen, client, mapper, dynamo_config);
										jsonresponse.put("response_status", "success");
										jsonresponse.put("user_jo", user_jo);
									}
									else if (method.equals("setUserPreference")) // email, this_access_token, target_email (of user to get) // also an admin method
									{
										 //System.out.println("Endpoint setUserPreference() begin: which=" + which + " and value=" + value);
										 String which = request.getParameter("which");
										 String value = request.getParameter("value");
										 if(which == null || value == null)
										 {
											 jsonresponse.put("message", "Invalid parameters.");
											 jsonresponse.put("response_status", "error");
										 }
										 else
										 {	 
											 jsonresponse.put("response_status", "success"); // default to success, then overwrite with error if necessary
											 if(which.equals("hide_hn_new") || which.equals("hide_hn_threads") || which.equals("hide_hn_comments") || which.equals("hide_hn_show") ||
													 which.equals("hide_hn_ask") || which.equals("hide_hn_jobs") || which.equals("hide_hn_submit") || which.equals("hide_hn_feed")  || which.equals("hide_hn_notifications"))
											 {
												 if(value.equals("show") || value.equals("hide"))
												 {
													 boolean yo = true;
													 if(value.equals("hide"))
														 yo = false;
													 if(which.equals("hide_hn_new"))
														 useritem.setHideHNNew(yo);
													 else if(which.equals("hide_hn_threads"))
														 useritem.setHideHNThreads(yo);
													 else if(which.equals("hide_hn_comments"))
														 useritem.setHideHNComments(yo);
													 else if(which.equals("hide_hn_show"))
														 useritem.setHideHNShow(yo);
													 else if(which.equals("hide_hn_ask"))
														 useritem.setHideHNAsk(yo);
													 else if(which.equals("hide_hn_jobs"))
														 useritem.setHideHNJobs(yo);
													 else if(which.equals("hide_hn_submit"))
														 useritem.setHideHNSubmit(yo);
													 else if(which.equals("hide_hn_feed"))
														 useritem.setHideHNFeed(yo);
													 else if(which.equals("hide_hn_notifications"))
														 useritem.setHideHNNotifications(yo);
													 mapper.save(useritem);	
													 jsonresponse.put("response_status", "success"); 
												 }
												 else
												 {
													 jsonresponse.put("message", "Invalid value.");
													 jsonresponse.put("response_status", "error");
												 }
											 }
											 else if(which.equals("onreply") || which.equals("onlike") 
													 || which.equals("ondislike"))
											 {
												 if(value.equals("button") || value.equals("do nothing"))
												 {
													 if(which.equals("onreply"))
														 useritem.setOnReply(value);
													 else if(which.equals("onlike"))
														 useritem.setOnLike(value);
													 else if(which.equals("ondislike"))
														 useritem.setOnDislike(value);
													 mapper.save(useritem);	
													 jsonresponse.put("response_status", "success"); 
												 }
												 else
												 {
													 jsonresponse.put("message", "Invalid value.");
													 jsonresponse.put("response_status", "error");
												 }
											 }
											 else if(which.equals("url_checking_mode")) 
											 {
												 if(value.equals("notifications_only"))
													 useritem.setURLCheckingMode("notifications_only");
												 else // this is an error, default to 450
													 useritem.setURLCheckingMode("stealth");
												 mapper.save(useritem);
												 jsonresponse.put("response_status", "success"); 
											 }
											 else if(which.equals("notification_mode")) 
											 {
												 if(value.equals("notifications_only"))
													 useritem.setNotificationMode("notifications_only");
												 else if(value.equals("newsfeed_and_notifications"))
													 useritem.setNotificationMode("newsfeed_and_notifications");
												 mapper.save(useritem);
												 jsonresponse.put("response_status", "success"); 
											 }
											 else
											 {
												 jsonresponse.put("message", "Invalid which value.");
												 jsonresponse.put("response_status", "error");
											 }
										 }
									 }
									 else if (method.equals("resetNotificationCount"))
									 {
										 //System.out.println("Endpoint resetNotificationCount() begin);
										 useritem.setNotificationCount(0);
										 mapper.save(useritem);
										 jsonresponse.put("message", "Notification count successfully reset."); 
										 jsonresponse.put("response_status", "success");
										//System.out.println("Endpoint resetNotificationCount() end);
									 }
									 else if (method.equals("resetNewsfeedCount"))
									 {
										 //System.out.println("Endpoint resetNewsfeedCount() begin);
										 useritem.setNewsfeedCount(0);
										 mapper.save(useritem);
										 jsonresponse.put("message", "Newsfeed count successfully reset."); 
										 jsonresponse.put("response_status", "success");
										//System.out.println("Endpoint resetNewsfeedCount() end);
									 }
									 else if (method.equals("removeItemFromNotificationIds"))
									 {
										 //System.out.println("Endpoint.removeItemFromNotificationIds() begin");
										 Set<String> notificationset = useritem.getNotificationIds();
										 if(notificationset != null)
										 {
											 notificationset.remove(request.getParameter("id"));
											 if(notificationset.isEmpty())
												 notificationset = null;
											 useritem.setNotificationIds(notificationset);
											 mapper.save(useritem);
										 }
										 // else notification set was already null, no need to return an error.
										 jsonresponse.put("response_status", "success");
										 //System.out.println("Endpoint.removeItemFromNotificationIds() end");
									 }
									 else if (method.equals("getNotificationItem"))
									 {
										 //System.out.println("Endpoint.getNotificationItem() begin");
										 String notification_id = request.getParameter("notification_id");
										 if(notification_id == null)
										 {
											 jsonresponse.put("message", "This method requires a notification_id value != null");
											 jsonresponse.put("response_status", "error"); 
										 }
										 else if(notification_id.isEmpty())
										 {
											 jsonresponse.put("message", "This method requires a non-empty notification_id value");
											 jsonresponse.put("response_status", "error"); 
										 }
										 else
										 {
											 NotificationItem ai = mapper.load(NotificationItem.class, notification_id, dynamo_config);
											 if(ai == null)
											 {
												 jsonresponse.put("message", "No notification with that ID exists.");
												 jsonresponse.put("response_status", "error"); 
											 }
											 else 
											 {
												 if(!ai.getUserId().equals(screenname))
												 {
													 jsonresponse.put("message", "Permission denied. You're not the owner of this notification.");
													 jsonresponse.put("response_status", "error"); 
												 }
												 else
												 {
													 jsonresponse.put("response_status", "success");
													 jsonresponse.put("notification_jo", ai.getJSON());
												 }
											 }
										 }
										 //System.out.println("Endpoint.getNotificationItem() end");
									 }
									 else if (method.equals("followUser"))
									 {
										 //System.out.println("Endpoint.followUser() begin");
										 String target_screenname = request.getParameter("target_screenname");
										 if(target_screenname == null)
										 {
											 jsonresponse.put("message", "This method requires a target_screenname value != null");
											 jsonresponse.put("response_status", "error"); 
										 }
										 else if(target_screenname.isEmpty())
										 {
											 jsonresponse.put("message", "This method requires a non-empty target_screenname value");
											 jsonresponse.put("response_status", "error"); 
										 }
										 else if(target_screenname.equals(screenname))
										 {
											 jsonresponse.put("message", "You can't follow yourself.");
											 jsonresponse.put("response_status", "error"); 
										 }
										 else if(useritem.getFollowing() != null && useritem.getFollowing().contains(target_screenname))
										 {
											 jsonresponse.put("message", "You are already following that user.");
											 jsonresponse.put("response_status", "error"); 
										 }
										 else
										 {
											 UserItem target_useritem = getUserInDBCreatingIfNotAndFoundWithinHNAPI(target_screenname);
											 if(target_useritem != null)
											 { 
												 System.out.println(useritem.getId() + " has chosen to follow " + target_useritem.getId());
												 // create "someone followed you" notification item and add to the user being followed, but only if he/she 
												 // (a) is registered and (b) has not already notified of this follow
												 if(target_useritem.getRegistered()) // (a) registered
												 {	
													 System.out.println(target_useritem.getId() + " was found in the DB and is registered with Hackbook.");
													 boolean already_notified = false;
													 HashSet<String> notification_item_ts = (HashSet<String>)target_useritem.getNotificationIds();
													 if(notification_item_ts == null || notification_item_ts.isEmpty())
														 already_notified = false;
													 else
													 {	 
														 Iterator<String> it = notification_item_ts.iterator();
														 NotificationItem ni = null;
														 while(it.hasNext())
														 {
															 ni = mapper.load(NotificationItem.class, it.next(), dynamo_config);
															 if(ni.getType().equals("0") && ni.getTriggerer().equals(useritem.getId()))
															 {
																 System.out.println("***" + target_useritem.getId() + " has already been notified that " + useritem.getId() + " is following them!");
																 already_notified = true;
																 break;
															 }
														 }
													 }
													 
													 if(!already_notified) // (b) not already notified
													 { 
														 long now = System.currentTimeMillis();
														 String now_str = Global.fromDecimalToBase62(7,now);
														 Random generator = new Random(); 
														 int r = generator.nextInt(238327); // this will produce numbers that can be represented by 3 base62 digits
														 String randompart_str = Global.fromDecimalToBase62(3,r);
														 String notification_id = now_str + randompart_str + "0"; 
															
														 NotificationItem ai = new NotificationItem(); 
														 ai.setId(notification_id);
														 ai.setActionMSFE(now);
														 ai.setMSFE(now);
														 ai.setUserId(target_useritem.getId());
														 ai.setType("0");
														 //ai.setHNTargetId(null);
														 ai.setTriggerer(useritem.getId());
														 //ai.setHNRootId(hn_target_id);
														 mapper.save(ai);
														 System.out.println("notification item " + notification_id + " has been saved in the db.");
														 
														 TreeSet<String> notificationset = new TreeSet<String>();
														 if(target_useritem.getNotificationIds() != null)
															 notificationset.addAll(target_useritem.getNotificationIds());
														 notificationset.add(notification_id);
														 while(notificationset.size() > Global.NOTIFICATIONS_SIZE_LIMIT)
													    		notificationset.remove(notificationset.first());
														 target_useritem.setNotificationIds(notificationset);
														 target_useritem.setNotificationCount(target_useritem.getNotificationCount()+1);
													 }
												 }
												 else
												 {
													 System.out.println(target_useritem.getId() + " was found in the DB but is NOT registered with Hackbook.");
												 }
												 
												 Set<String> followersset = target_useritem.getFollowers();
												 if(followersset == null)
													 followersset = new HashSet<String>();
												 followersset.add(useritem.getId()); // add useritem to the target_useritem's followers list
												 target_useritem.setFollowers(followersset);
												 mapper.save(target_useritem);
												 System.out.println(target_useritem.getId() + " has been saved with new followers list and notification id (if registered)");
												 
												 Set<String> followingset = useritem.getFollowing();
												 if(followingset == null)
													 followingset = new HashSet<String>();
												 followingset.add(target_useritem.getId()); // add target_useritem to the useritem's following list
												 useritem.setFollowing(followingset);
												 
												 // look up what this target_user has done over the past 2 days...
												 HashSet<HNItemItem> hnitems = target_useritem.getHNItemsByd(2880, mapper, dynamo_config); // 2 days
												 Set<String> newsfeed_ids = useritem.getNewsfeedIds();
												 if(hnitems != null)
												 { 
													 System.out.println("Found " + hnitems.size() + " items by " + target_useritem.getId() + " in the past 2 days.");
													 Iterator<HNItemItem> hnitem_it = hnitems.iterator();
													 HNItemItem current = null;
													 NotificationItem ni = null;
													 long now = 0L;
													 String now_str = "";
													 Random generator = new Random(); 
													 int r = 0;; // this will produce numbers that can be represented by 3 base62 digits
													 String randompart_str = "";
													 String notification_id = ""; 
													
													 if(newsfeed_ids == null)
														 newsfeed_ids = new HashSet<String>();
													 while(hnitem_it.hasNext())
													 {
														 current = hnitem_it.next();
														 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
														 sdf.setTimeZone(TimeZone.getTimeZone("America/New_York"));
														 Calendar cal = Calendar.getInstance();
														 cal.add(Calendar.MINUTE, -2880);
														 System.out.println("Found a " + current.getType() + " by " + current.getBy() + " at " + current.getTime() * 1000 + " hr=" + sdf.format(current.getTime() * 1000) + " cutoff=" + sdf.format(cal.getTimeInMillis()));
														 if(current.getType().equals("comment") || current.getType().equals("story")) // If this is a comment or story, then it fits. ignore everything else
														 { 
															 r = generator.nextInt(238327); // this will produce numbers that can be represented by 3 base62 digits
															 now = System.currentTimeMillis();
															 now_str = Global.fromDecimalToBase62(7,(current.getTime()*1000));
															 randompart_str = Global.fromDecimalToBase62(3,r);
															 ni = new NotificationItem();
															 if(current.getType().equals("comment")) // if it's a comment, step back and find root
															 {
																 notification_id = now_str + randompart_str + "8";
																 ni.setType("8");
																 long root = Global.findRootItem(current.getId());
																 if(root == -1)
																	 break; // couldn't find root, so bail
																 else
																	 ni.setHNRootId(root);
															 }
															 else if(current.getType().equals("story")) // // if it's a story, root is the same as the item id
															 {
																 notification_id = now_str + randompart_str + "7";
																 ni.setType("7");
																 ni.setHNRootId(current.getId());
															 }
															 ni.setId(notification_id);
															 ni.setActionMSFE(current.getTime()*1000);
															 ni.setMSFE(now);
															 ni.setUserId(useritem.getId());
															 ni.setHNTargetId(current.getId());
															 ni.setTriggerer(target_useritem.getId());
															 mapper.save(ni);							
															 newsfeed_ids.add(notification_id);
														 }
													 }
												 }
												 // if empty, set to null. If more than max, get most recent Global.NEWSFEED_SIZE_LIMIT items
												 if(newsfeed_ids.isEmpty())
													 newsfeed_ids = null;
												 else if(newsfeed_ids.size() > Global.NEWSFEED_SIZE_LIMIT)
												 {
													 TreeSet<String> temp_ids = new TreeSet<String>();
													 temp_ids.addAll(newsfeed_ids);
													 newsfeed_ids = new TreeSet<String>(); // empty out the existing ids;
													 Iterator<String> it = temp_ids.descendingIterator();
													 int x = 0;
													 String currentstr = "";
													 while(x < Global.NEWSFEED_SIZE_LIMIT)
													 {
														 currentstr = it.next();
														 newsfeed_ids.add(currentstr);
														 x++;
													 }
													 if(useritem.getNewsfeedCount() > Global.NEWSFEED_SIZE_LIMIT) // count can't be more than the limit
														 useritem.setNewsfeedCount(Global.NEWSFEED_SIZE_LIMIT); // so set it to the limit
												 }
												 useritem.setNewsfeedIds(newsfeed_ids);
												 mapper.save(useritem);
												 System.out.println(useritem.getId() + " has been saved with new following list");
												 jsonresponse.put("response_status", "success");
											 }
											 else
											 {
												 jsonresponse.put("message", "Invalid user.");
												 jsonresponse.put("response_status", "error");  
											 }
										 }
										 //System.out.println("Endpoint.followUser() end");
									 }
									 else if (method.equals("unfollowUser"))
									 {
										 //System.out.println("Endpoint.unfollowUser() begin");
										 String target_screenname = request.getParameter("target_screenname");
										 UserItem target_useritem = mapper.load(UserItem.class, target_screenname, dynamo_config);
										 if(target_useritem == null)
										 {
											 jsonresponse.put("message", "Can't unfollow user bc they don't exist in the DB.");
											 jsonresponse.put("response_status", "error");
										 }
										 else if(target_screenname.isEmpty())
										 {
											 jsonresponse.put("message", "This method requires a non-empty target_screenname value");
											 jsonresponse.put("response_status", "error"); 
										 }
										 else if(target_screenname.equals(screenname))
										 {
											 jsonresponse.put("message", "You can't unfollow yourself.");
											 jsonresponse.put("response_status", "error"); 
										 }
										 else
										 {
											 Set<String> followingset = useritem.getFollowing();
											 if(followingset == null || !followingset.contains(target_screenname))
											 {
												 jsonresponse.put("message", "You aren't following that user.");
												 jsonresponse.put("response_status", "error"); 
											 }
											 else
											 {
												 followingset.remove(target_useritem.getId());
												 if(followingset.isEmpty())
													 followingset = null;
												 useritem.setFollowing(followingset);

												 // remove all newsfeed items triggered by the user we're unfollowing
												 Set<NotificationItem> newsfeedset = useritem.getNewsfeedItems(0, mapper, dynamo_config);
												 Iterator<NotificationItem> it0 = newsfeedset.iterator();
												 NotificationItem current = null;
												 Set<String> remaining_ids = new HashSet<String>();
												 while(it0.hasNext())
												 {
													 current = it0.next();
													// System.out.println("found newsfeed item triggered by:" + current.getTriggerer());
													 if(followingset.contains(current.getTriggerer()))
													 {
														 //System.out.println("\tmatch with" + target_useritem.getId());
														 remaining_ids.add(current.getId());
													 }
													// else no match
												 }
												 // if what's left after removing the unfollowed user's stuff is empty, set to null. If over the limit (not sure how), then resize to limit
												 if(remaining_ids.isEmpty())
													 remaining_ids = null;
												 else if(remaining_ids.size() > Global.NEWSFEED_SIZE_LIMIT) // if user is unfollowing someone, then I'm not sure it's possible for there to be more than the limit, but leave this here anyway
												 {
													 TreeSet<String> temp_ids = new TreeSet<String>();
													 temp_ids.addAll(remaining_ids);
													 remaining_ids = new TreeSet<String>(); // empty out the existing ids;
													 Iterator<String> it1 = temp_ids.descendingIterator();
													 int x = 0;
													 String currentstr = "";
													 while(x < Global.NEWSFEED_SIZE_LIMIT)
													 {
														 currentstr = it1.next();
														 remaining_ids.add(currentstr);
														 x++;
													 }
													 if(useritem.getNewsfeedCount() > Global.NEWSFEED_SIZE_LIMIT) // count can't be more than the limit
														 useritem.setNewsfeedCount(Global.NEWSFEED_SIZE_LIMIT); // so set it to the limit
												 }
												 useritem.setNewsfeedIds(remaining_ids);
												 mapper.save(useritem);
												 
												 // remove useritem from target_useritem's followers set
												 Set<String> followersset = target_useritem.getFollowers();
												 if(followersset != null)
												 {
													 followersset.remove(useritem.getId());
													 if(followersset.isEmpty())
														 followersset = null;
													 target_useritem.setFollowers(followersset);
													 mapper.save(target_useritem);
												 }
												 // don't need to remove anything from notifications because notifications are based on 
												 // stuff that is done to the user, not who the user is following
												 jsonresponse.put("response_status", "success");
											 }
										 }
									 }
								 }
								 else // user had an screenname and this_access_token, but they were not valid. Let the frontend know to get rid of them
								 {
									 jsonresponse.put("response_status", "error");
									 jsonresponse.put("message", "screenname + access token present, but not valid. Please try again.");
									 jsonresponse.put("error_code", "0000");
								 }
							 }
							 else // couldn't get useritem from provided screenname
							 {
								 jsonresponse.put("response_status", "error");
								 jsonresponse.put("message", "No user was found for that screenname. Please try again.");
								 jsonresponse.put("error_code", "0000");
							 }
					 	}
					 	else // either screenname or tat was null, but not both
					 	{
					 		jsonresponse.put("response_status", "error");
					 		jsonresponse.put("message", "screenname or access token was null. Please try again.");
					 		jsonresponse.put("error_code", "0000");
					 	}
					 }	
					 else // email and tat were both null
					 {
						 jsonresponse.put("response_status", "error");
						 jsonresponse.put("message", "You must be logged in to do that.");
					 }
				 }
				 else
				 {
					 jsonresponse.put("response_status", "error");
					 jsonresponse.put("message", "Unsupported method. method=" + method);
				 }
			}
			long timestamp_at_exit = System.currentTimeMillis();
			long elapsed = timestamp_at_exit - timestamp_at_entry;
			jsonresponse.put("elapsed", elapsed);
			jsonresponse.put("msfe", timestamp_at_exit);
			if(method != null)
				jsonresponse.put("method", method);
			if(devel == true)
				System.out.println("response=" + jsonresponse);	// respond with object, success response, or error 
			out.println(jsonresponse);
		}
		catch(JSONException jsone)
		{
			out.println("{ \"response_status\": \"error\", \"message\": \"JSONException caught in Endpoint GET\"}");
			System.err.println("endpoint: JSONException thrown in large try block. " + jsone.getMessage());
		}		
		return; 	
	}

	public UserItem getUserInDBCreatingIfNotAndFoundWithinHNAPI(
			String target_screenname) {
		UserItem target_useritem = mapper.load(UserItem.class,
				target_screenname, dynamo_config);
		if (target_useritem == null) // not in local db, try hacker news API
		{
			try {
				Response r = Jsoup
						.connect(
								"https://hacker-news.firebaseio.com/v0/user/"
										+ target_screenname + ".json")
						.userAgent(
								"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36")
						.ignoreContentType(true).execute();
				// if r failed, we skip to the catch rather than creating a user
				String result = r.body();
				if (result == null || result.trim().isEmpty()
						|| result.trim().equals("null")) {
					return null;
				} else {
					target_useritem = new UserItem();
					target_useritem.setHNProfile(result);
					JSONObject profile_jo = new JSONObject(result);
					target_useritem.setHNKarma(profile_jo.getInt("karma"));
					target_useritem.setLastKarmaCheck(System
							.currentTimeMillis());
					target_useritem.setHNSince(profile_jo.getLong("created"));
					target_useritem.setId(profile_jo.getString("id"));
					target_useritem.setRegistered(false);
					target_useritem.setURLCheckingMode("stealth");
					if (profile_jo.has("about"))
						target_useritem.setHNAbout(profile_jo
								.getString("about"));
					else
						target_useritem.setHNAbout("");
					mapper.save(target_useritem);
					return target_useritem;
				}
			} catch (IOException ioe) {
				return null;
			} catch (JSONException jsone) {
				return null;
			}
		} else {
			return target_useritem;
		}
	}

}
