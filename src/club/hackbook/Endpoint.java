package club.hackbook;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
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
			tempset = getHNItemsFromURL(url_str + "/", minutes_ago); // as-is + trailing "/"
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
						"http://www." + url_str.substring(12), minutes_ago); // try http && www
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
						"http://www." + url_str.substring(8), minutes_ago); // try http && www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
				tempset = getHNItemsFromURL(
						"https://www." + url_str.substring(8), minutes_ago); // try https && www
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
						"https://www." + url_str.substring(11), minutes_ago); // try https && www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
			} else if (!url_str.startsWith("http://www.")) // http & !www.
			{
				tempset = getHNItemsFromURL(
						"http://www." + url_str.substring(7), minutes_ago); // try http && www
				if (tempset != null) {
					combinedset.addAll(tempset); // try http && !www
					tempset = null;
				}
				tempset = getHNItemsFromURL(
						"https://www." + url_str.substring(7), minutes_ago); // try https && www
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
		response.setContentType("application/json; charset=UTF-8;");
		response.setHeader("Access-Control-Allow-Origin","*"); //FIXME ?
		PrintWriter out = response.getWriter();
		JSONObject jsonresponse = new JSONObject();
		long timestamp_at_entry = System.currentTimeMillis();
		String method = request.getParameter("method");
		if(!request.isSecure() && !(devel == true && request.getRemoteAddr().equals("127.0.0.1")))
		{
			try 
			{
				jsonresponse.put("message", "This API endpoint must be communicated with securely.");
				jsonresponse.put("response_status", "error");
			}
			catch(JSONException jsone)
			{
				out.println("{ \"response_status\": \"error\", \"message\": \"JSONException caught in Endpoint GET secure connection check. method=" + method + "\"}");
				System.err.println("endpoint: JSONException thrown in Endpoint GET secure connection check. " + jsone.getMessage());
				jsone.printStackTrace();
				return;
			}	
		}
		else if(method == null)
		{
			try 
			{
				jsonresponse.put("message", "Method not specified. This should probably produce HTML output reference information at some point.");
				jsonresponse.put("response_status", "error");
			}
			catch(JSONException jsone)
			{
				out.println("{ \"response_status\": \"error\", \"message\": \"JSONException caught in Endpoint GET method value sanity check.\"}");
				System.err.println("endpoint: JSONException thrown in Endpoint GET method value sanity check. " + jsone.getMessage());
				jsone.printStackTrace();
				return;
			}	
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
			if(method.equals("searchForHNItem") || method.equals("getHNAuthToken") || method.equals("verifyHNUser") || method.equals("getMostFollowedUsers"))
			{
				try 
				{
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
								useritem.setHideEmbeddedCounts(true);
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
						String topcolor = request.getParameter("topcolor");
						if(screenname == null || screenname.isEmpty())
						{
							jsonresponse.put("message", "Screenname was null or empty.");
							jsonresponse.put("response_status", "error");
						}
						else 
						{
							UserItem useritem = mapper.load(UserItem.class, screenname, dynamo_config);
							if(useritem == null)	// if the user has gotten to verifyHNUser, then the useritem stub should have just been created at getHNAuthToken. Fail if not.
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
								
								// wait 11 seconds to do first try. This helps prevent read or socket timeout errors on tries 0, 1 and 2 which are unlikely to work anyway.
								try {
									java.lang.Thread.sleep(11000);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								x=2;
								while(x < limit) // 2 (11 sec), 3 (16 sec), 4 (21 sec), 5 (26 sec), 6 (31 sec)
								{
									try
									{
										result = Jsoup
												 .connect("https://hacker-news.firebaseio.com/v0/user/" + screenname  + ".json")
												 .userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36")
												 .ignoreContentType(true).execute().body();
										
										System.out.println("Endpoint.verifyHNUser():" + result);
										if(result == null || result.equals("null") || result.isEmpty())
										{
											jsonresponse.put("response_status", "error");
											jsonresponse.put("message", "Hackbook encountered an error attempting to validate you with the HN API. If you are a new user or one with low karma, the HN API does not recognize you and Hackbook will not be able to verify your account. Sorry.");
											break;
										}
										else
										{	
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
															useritem.setNotificationCount(0);
															useritem.setPermissionLevel("user");
															useritem.setId(screenname);
															useritem.setSince(now);
															useritem.setSinceHumanReadable(sdf.format(now));
															useritem.setRegistered(true);
															useritem.setURLCheckingMode("stealth");
															useritem.setHideEmbeddedCounts(true);
														}
														if(topcolor != null && isValidTopcolor(topcolor))
															useritem.setHNTopcolor(topcolor);
														else
															useritem.setHNTopcolor("ff6600");
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
				}
				catch(JSONException jsone)
				{
					out.println("{ \"response_status\": \"error\", \"message\": \"JSONException caught in Endpoint GET non-auth methods. method=" + method + "\"}");
					System.err.println("endpoint: JSONException thrown in Endpoint GET non-auth methods. " + jsone.getMessage());
					jsone.printStackTrace();
					return;
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
					 method.equals("resetNewsfeedCount") || method.equals("getChat"))
			 {
				 try
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
										boolean something_needs_updating = false;
									
										// check ext version as reported by user.
										String ext_version = request.getParameter("ext_version");
										if(ext_version != null && !ext_version.isEmpty()) // covering the bases
										{
											if(ext_version.length() == 5 && Global.isWholeNumeric(ext_version.substring(0,1)) && ext_version.substring(1,2).equals(".") && Global.isWholeNumeric(ext_version.substring(2,5))) // is of the form "X.YYY"
											{
												if(useritem.getExtVersion() == null || !useritem.getExtVersion().equals(ext_version)) // if the existing value is null, or the verions don't match, update
												{
													useritem.setExtVersion(ext_version);
													something_needs_updating = true;
												}
											}
										}
										
										JSONObject user_jo = null;
										long now = System.currentTimeMillis();
										
										if(now - useritem.getSeen() > 600000) // if it's been > 10 mins since last "seen" update, update it
										{
											something_needs_updating = true;
											useritem.setSeen(now);
											SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
											sdf.setTimeZone(TimeZone.getTimeZone("America/New_York"));
											useritem.setSeenHumanReadable(sdf.format(timestamp_at_entry));
											
											// Also update karma here, although FirebaseListener should be keeping track of all changes.
											// so this is not entirely necessary. It's only necessary if FBL isn't doing its job (i.e. missing karma changes)
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
										}

										if(something_needs_updating)
											mapper.save(useritem);
										
										user_jo = useritem.getAsJSONObject(client, mapper, dynamo_config);
										
										GlobalvarItem gvi = mapper.load(GlobalvarItem.class, "latest_ext_version", dynamo_config);
										if(gvi != null)
											jsonresponse.put("latest_ext_version", gvi.getStringValue());
										jsonresponse.put("response_status", "success");
										jsonresponse.put("user_jo", user_jo);
									}
									else if (method.equals("setUserPreference")) // email, this_access_token, target_email (of user to get) // also an admin method
									{
										 String which = request.getParameter("which");
										 String value = request.getParameter("value");
										 if(which == null || value == null)
										 {
											 jsonresponse.put("message", "Invalid parameters.");
											 jsonresponse.put("response_status", "error");
										 }
										 else
										 {	 
											 System.out.println("Endpoint setUserPreference() begin: which=" + which + " and value=" + value);
											 jsonresponse.put("response_status", "success"); // default to success, then overwrite with error if necessary
											 if(which.equals("url_checking_mode")) 
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
											 else if(which.equals("karma_pool_ttl")) 
											 {
												 if(!Global.isWholeNumeric(value))
												 {
													 jsonresponse.put("message", "Must be an int between 5 and 1440.");
													 jsonresponse.put("response_status", "error");
												 }
												 else
												 {
													 int val = Integer.parseInt(value);
													 if(val > 1440 || val < 5)
													 {
														 jsonresponse.put("message", "Must be an int between 5 and 1440.");
														 jsonresponse.put("response_status", "error");
													 }
													 else
													 {
														 useritem.setKarmaPoolTTLMins(val);
														 mapper.save(useritem);
														 jsonresponse.put("response_status", "success"); 
													 }
												 }
											 }
											 else if(which.equals("hide_embedded_counts") || which.equals("hide_inline_follow") || which.equals("hide_deep_reply_notifications")) 
											 {
												 if(value.equals("show") || value.equals("hide"))
												 {
													 if(which.equals("hide_embedded_counts"))
													 {
														 if(value.equals("show"))
															 useritem.setHideEmbeddedCounts(false);
														 else
															 useritem.setHideEmbeddedCounts(true);
													 }
													 
													 else if(which.equals("hide_inline_follow"))
													 {
														 if(value.equals("show"))
															 useritem.setHideInlineFollow(false);
														 else
															 useritem.setHideInlineFollow(true);
													 }
													 else if(which.equals("hide_deep_reply_notifications"))
													 {
														 if(value.equals("show"))
															 useritem.setHideDeepReplyNotifications(false);
														 else
															 useritem.setHideDeepReplyNotifications(true);
													 }
													 mapper.save(useritem);
												 }
												 else
												 {
													 jsonresponse.put("message", "Must be \"show\" or \"hide\".");
													 jsonresponse.put("response_status", "error");
												 }
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
												 // No notification with the specified ID exists. 
												 // If that id is in the user's notification or newsfeed sets, remove it and update user.
												 boolean saveuser = false;
												 Set<String> notificationset = useritem.getNotificationIds();
												 if(notificationset != null && notificationset.contains(notification_id))
												 {
													 notificationset.remove(notification_id);
													 if(notificationset.isEmpty())
														 notificationset = null;
													 useritem.setNotificationIds(notificationset);
													 saveuser = true;
												 }
												 Set<String> newsfeedset = useritem.getNewsfeedIds();
												 if(newsfeedset != null && newsfeedset.contains(notification_id))
												 {
													 newsfeedset.remove(notification_id);
													 if(newsfeedset.isEmpty())
														 newsfeedset = null;
													 useritem.setNewsfeedIds(newsfeedset);
													 saveuser = true;
												 }
												 if(saveuser)
													 mapper.save(useritem);
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
														 //ai.setHNRootStoryId();
														 //ai.setHNRootCommentId();
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
												 mapper.save(useritem);
												 System.out.println(useritem.getId() + " has been saved with a new following list");
												 
												 HashSet<HNItemItem> hnitems = target_useritem.getHNItemsByd(2880, mapper, dynamo_config); // 2 days
												 if(hnitems != null && !hnitems.isEmpty())
												 {	 
													 NewFollowNewsfeedAdjuster nfnfa = new NewFollowNewsfeedAdjuster(useritem, target_useritem, hnitems, mapper, dynamo_config);
													 if(hnitems.size() > 5) // people like jacquesm and tptacek can have 25+ items in 2 days
													 {
														 System.out.println("Doing it ASYNCHRONOUSLY");
														 nfnfa.start(); // ASYNCHRONOUSLY put new feed items (triggered by target_useritem) into useritem's feed
													 }
													 else
													 {
														 System.out.println("Doing it SYNCHRONOUSLY");
														 nfnfa.run(); // SYNCHRONOUSLY put new feed items into useritem's feed. When this returns and getUserSelf() fires, everything will be up-to-date wherever the user may click
													 }
												 }

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
										 System.out.println("Endpoint.unfollowUser() begin");
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
											 System.out.println("inc values ok");
											 Set<String> followingset = useritem.getFollowing();
											 if(followingset == null || !followingset.contains(target_screenname))
											 {
												 jsonresponse.put("message", "You aren't following that user.");
												 jsonresponse.put("response_status", "error"); 
											 }
											 else
											 {
												
												 System.out.println("You are following that user, so it can be removed right now.");
												 followingset.remove(target_useritem.getId());
												 if(followingset.isEmpty())
													 followingset = null;
												 useritem.setFollowing(followingset);
												 
												 Set<String> newsfeedset = useritem.getNewsfeedIds();
												 TreeSet<String> new_newsfeedset = new TreeSet<String>();
												 if(newsfeedset != null && !newsfeedset.isEmpty())
												 {	 
													 Iterator<String> newsfeed_it = newsfeedset.iterator();
													 String table1Name = "hackbook_notifications2";
													 HashMap<String, KeysAndAttributes> requestItems = new HashMap<String, KeysAndAttributes>();
													 ArrayList<Map<String, AttributeValue>> keys1 = new ArrayList<Map<String, AttributeValue>>();
													 HashMap<String, AttributeValue> table1key1 = null;
													 while(newsfeed_it.hasNext())
													 {	 
														 table1key1 = new HashMap<String, AttributeValue>();
														 table1key1.put("id", new AttributeValue().withS(newsfeed_it.next()));
														 keys1.add(table1key1);
													 }
													 requestItems.put(table1Name, new KeysAndAttributes().withKeys(keys1));    	
													 BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest().withRequestItems(requestItems);
													 BatchGetItemResult result = client.batchGetItem(batchGetItemRequest);

													 List<Map<String,AttributeValue>> table1Results = result.getResponses().get(table1Name);
													 System.out.println("Items in table " + table1Name);
													 //NotificationItem ni = null;
													 for (Map<String,AttributeValue> item : table1Results) {
														 if(!item.get("triggerer").getS().equals(target_screenname)) // only keep the ones that weren't triggered by the user that just got unfollowed
														 {	 
															 // don't have to actually create the NotificationItem here, but leave this for cut paste later.
															 /* ni = new NotificationItem();
															 ni.setId(item.get("id").getS());
															 ni.setUserId(item.get("user_id").getS());
															 ni.setType(item.get("type").getS());
															 ni.setTriggerer(item.get("triggerer").getS());
															 ni.setActionMSFE(Long.parseLong(item.get("action_msfe").getN()));
															 ni.setMSFE(Long.parseLong(item.get("msfe").getN()));
															 ni.setHNTargetId(Long.parseLong(item.get("hn_target_id").getN()));
															 ni.setHNRootStoryId(Long.parseLong(item.get("hn_root_story_id").getN()));
															 ni.setHNRootCommentId(Long.parseLong(item.get("hn_root_comment_id").getN()));
															 ni.setKarmaChange(Integer.parseInt(item.get("karma_change").getN()));*/
															 new_newsfeedset.add(item.get("id").getS());
														 }
													 }
													 if(new_newsfeedset.isEmpty())
														 new_newsfeedset = null;
													 useritem.setNewsfeedIds(new_newsfeedset);
												 }
												 mapper.save(useritem);
												 // 1. we don't have to check NotificationIds because this is an unfollow which only affects newsfeed items.
												 // 2. We don't need to check newsfeed size limit because we're unfollowing. At worst, the size stays the same.
												
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
										 System.out.println("Endpoint.unfollowUser() end");
									 }
									 else if (method.equals("addMessageToChat"))
									 {
										 String message = request.getParameter("message");
										 if(message != null && message.isEmpty())
										 {
											 jsonresponse.put("message", "Message was empty.");
											 jsonresponse.put("response_status", "error"); 
										 }
										 else
										 {
											 ChatItem ci = new ChatItem();
											 long now = System.currentTimeMillis();
											 String now_str = Global.fromDecimalToBase62(7,now);
											 Random generator = new Random(); 
											 int r = generator.nextInt(238327); // this will produce numbers that can be represented by 3 base62 digits
											 String randompart_str = Global.fromDecimalToBase62(3,r);
											 String message_id = now_str + randompart_str;
											 ci.setId(message_id);
											 ci.setUserId(useritem.getId());
											 ci.setHostname("news.ycombinator.com");
											 ci.setMSFE(now);
											 ci.setText(message);
											 mapper.save(ci);
											 jsonresponse.put("response_status", "success");
										 }
									 }
									 else if (method.equals("getChat"))
									 {
										 HashSet<ChatItem> chat = getChat(10080); // one week in minutes
										 Iterator<ChatItem> chat_it = chat.iterator();
										 ChatItem currentitem = null;
										 JSONArray chat_ja = new JSONArray();
										 while(chat_it.hasNext())
										 {
											 currentitem = chat_it.next();
											 chat_ja.put(currentitem.getJSON());
										 }
										 jsonresponse.put("response_status", "success");
										 if(chat_ja.length() > 0)
											 jsonresponse.put("chat_ja", chat_ja);
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
				 catch(JSONException jsone)
				 {
					 out.println("{ \"response_status\": \"error\", \"message\": \"JSONException caught in Endpoint GET methods requiring user auth.\"}");
					 System.err.println("endpoint: JSONException thrown in Endpoint GET methods requiring user auth. " + jsone.getMessage());
					 jsone.printStackTrace();
					 return;
				 }	
			 }
			 else
			 {
				 try
				 {
					 jsonresponse.put("response_status", "error");
					 jsonresponse.put("message", "Unsupported method. method=" + method);
				 }
				 catch(JSONException jsone)
				 {
					 out.println("{ \"response_status\": \"error\", \"message\": \"JSONException caught in Endpoint GET unsupported method response generation.\"}");
					 System.err.println("endpoint: JSONException thrown in Endpoint GET unsupported method response generation. " + jsone.getMessage());
					 jsone.printStackTrace();
					 return;
				 }	
			 }
		}
		long timestamp_at_exit = System.currentTimeMillis();
		long elapsed = timestamp_at_exit - timestamp_at_entry;
		try{
			jsonresponse.put("elapsed", elapsed);
			jsonresponse.put("msfe", timestamp_at_exit);
			if(method != null)
				jsonresponse.put("method", method);
		}
		catch(JSONException jsone)
		{
			out.println("{ \"response_status\": \"error\", \"message\": \"JSONException caught in Endpoint GET elapsed and msfe generation.\"}");
			System.err.println("endpoint: JSONException thrown in Endpoint GET elapsed and msfe generation. " + jsone.getMessage());
			jsone.printStackTrace();
			return;
		}	
		if(devel == true)
			System.out.println("response=" + jsonresponse);	// respond with object, success response, or error 
		out.println(jsonresponse);	
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
					JSONObject profile_jo = new JSONObject(result);
					target_useritem.setHNKarma(profile_jo.getInt("karma"));
					target_useritem.setHNSince(profile_jo.getLong("created"));
					target_useritem.setId(profile_jo.getString("id"));
					target_useritem.setRegistered(false); 
					target_useritem.setHideEmbeddedCounts(true);
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
	
	private HashSet<ChatItem> getChat(int minutes_ago) 
	{
		DynamoDBQueryExpression<ChatItem> queryExpression = new DynamoDBQueryExpression<ChatItem>()
				.withIndexName("hostname-msfe-index").withScanIndexForward(true)
				.withConsistentRead(false);

		// set the user_id part
		ChatItem key = new ChatItem();
		key.setHostname("news.ycombinator.com");
		queryExpression.setHashKeyValues(key);

		if (minutes_ago > 0) {
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.MINUTE, (minutes_ago * -1));
			long msfe_cutoff = cal.getTimeInMillis();
			// set the msfe range part
			Map<String, Condition> keyConditions = new HashMap<String, Condition>();
			keyConditions.put("msfe", new Condition().withComparisonOperator(ComparisonOperator.GT).withAttributeValueList(new AttributeValue().withN(new Long(msfe_cutoff).toString())));
			queryExpression.setRangeKeyConditions(keyConditions);
		}

		// execute
		List<ChatItem> items = mapper.query(ChatItem.class, queryExpression, dynamo_config);
		if (items != null && items.size() > 0) {
			HashSet<ChatItem> returnset = new HashSet<ChatItem>();
			for (ChatItem item : items) {
				returnset.add(item);
			}
			return returnset;
		} else {
			return null;
		}
	}
	
	public boolean isValidTopcolor(String color)
	{
		// 3color "^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$";
		Pattern pattern = Pattern.compile("^[A-Fa-f0-9]{6}$");
		Matcher matcher = pattern.matcher(color);
		return matcher.matches();
	}

}
