package club.hackbook;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.TimeZone;
import java.util.HashSet;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class PeriodicCalculator extends java.lang.Thread {

	private DynamoDBMapper mapper;
	private DynamoDBMapperConfig dynamo_config;
	private AmazonDynamoDBClient client;
	
	public void initialize()
	{
	}
	
	public PeriodicCalculator(DynamoDBMapper inc_mapper, DynamoDBMapperConfig inc_dynamo_config, AmazonDynamoDBClient inc_client)
	{
		this.initialize();
		mapper = inc_mapper;
		dynamo_config = inc_dynamo_config;
		client = inc_client;
	}
	
	long nextLong(Random rng, long n) {
		   // error checking and 2^x checking removed for simplicity.
		   long bits, val;
		   do {
		      bits = (rng.nextLong() << 1) >>> 1;
		      val = bits % n;
		   } while (bits-val+(n-1) < 0L);
		   return val;
		}
	
	public void run()
	{
		System.out.println("=== " + super.getId() +  " Fired a PeriodicCalculator thread.");
		long entry = System.currentTimeMillis();
		
		// first set last timestamp to prevent others from firing
		GlobalvarItem periodic_last_msfe_gvi = mapper.load(GlobalvarItem.class, "periodic_last_msfe", dynamo_config);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
		sdf.setTimeZone(TimeZone.getTimeZone("America/New_York"));
		periodic_last_msfe_gvi.setStringValue(sdf.format(entry));
		periodic_last_msfe_gvi.setNumberValue(entry);
		mapper.save(periodic_last_msfe_gvi);

		int limit = 42;
		
		// get user table size
		// this isn't exact... updated hourly within Dynamo, but good enough.
		TableDescription tableDescription = client.describeTable(new DescribeTableRequest().withTableName("hackbook_users2")).getTable();
		long tablesize = tableDescription.getItemCount();
		
		// create a set of <limit> random longs
		/*HashSet<Long> randomlongs = new HashSet<Long>();
		Random generator = new Random(); 
		long randomlong = 0L; // this will produce numbers that can be represented by 3 base62 digits
		while(randomlongs.size() < limit) // this will keep adding until there are > limit unique longs in the set
		{
			randomlong = nextLong(generator, tablesize);
			randomlongs.add(randomlong); 
			System.out.println(randomlong);
		}*/
		
		DynamoDBScanExpression userScanExpression = new DynamoDBScanExpression();
		List<UserItem> userScanResult = mapper.scan(UserItem.class, userScanExpression, dynamo_config);
	
		Comparator<UserItem> comparator = new FollowerCountComparator();
		PriorityQueue<UserItem> queue = new PriorityQueue<UserItem>(limit, comparator);
		//HashSet<UserItem> random_users = new HashSet<UserItem>();
		Long x = 0L;
		for (UserItem useritem : userScanResult) { 
			
			// add the user to the priorityqueue
			queue.add(useritem);
			// if the queue is too big, remove one
			if(queue.size() > limit)
				queue.remove();
			
			// if this value of x is in the randomlongs set, add the useritem to the random_users set
		//	if(randomlongs.contains(x)) 
		//		random_users.add(useritem);
			x++;
		}
		
		System.out.println("Most followers: (size=" + queue.size() + ")");
		UserItem currentuseritem = null;
		JSONArray most_followed_users_ja = new JSONArray();
		JSONObject temp_jo = new JSONObject();
		try
		{
			// loop through the queue of most followed users, create a jo for each and add to the master ja
			while (queue.size() != 0)
			{
				currentuseritem = queue.remove();
				if(currentuseritem.getFollowers() != null)
					System.out.println("id=" + currentuseritem.getId() + " follower_count=" + currentuseritem.getFollowers().size());
				else
					System.out.println("id=" + currentuseritem.getId() + " follower_count=" + 0);
				temp_jo = new JSONObject();
				temp_jo.put("id", currentuseritem.getId());
				if(currentuseritem.getFollowers() == null)
					temp_jo.put("num_followers", 0);
				else
					temp_jo.put("num_followers", currentuseritem.getFollowers().size());
				most_followed_users_ja.put(temp_jo);
			}
		}
		catch(JSONException jsone)
		{
			System.err.println("JSONException trying to set global vars for random users and most followed users");
		}
		
		/*Iterator<UserItem> user_it = random_users.iterator();
		System.out.println("Random users:");
		JSONArray random_users_ja = new JSONArray();
		try
		{
			// loop through the random users, create a jo for each and add to the ja
			while(user_it.hasNext())
			{
				currentuseritem = user_it.next();
				System.out.println("id=" + currentuseritem.getId());
				temp_jo = new JSONObject();
				temp_jo.put("id", currentuseritem.getId());
				if(currentuseritem.getFollowers() == null)
					temp_jo.put("num_followers", 0);
				else
					temp_jo.put("num_followers", currentuseritem.getFollowers().size());
				random_users_ja.put(temp_jo);
			}
		}
		catch(JSONException jsone)
		{
			System.err.println("JSONException trying to set global vars for random users and most followed users");
		}*/
		
		System.out.println(most_followed_users_ja);
		//System.out.println(random_users_ja);
		
		// now save both jas to the database
		GlobalvarItem most_followed_users_gvi = mapper.load(GlobalvarItem.class, "most_followed_users", dynamo_config);
		//GlobalvarItem random_users_gvi = mapper.load(GlobalvarItem.class, "random_users", dynamo_config);
		most_followed_users_gvi.setStringValue(most_followed_users_ja.toString());
		//random_users_gvi.setStringValue(random_users_ja.toString());
		mapper.save(most_followed_users_gvi);
		//mapper.save(random_users_gvi);
		
		long exit = System.currentTimeMillis();
		System.out.println("=== " + super.getId() +  " PeriodicCalculator Done. elapsed=" + ((exit - entry)/1000) + "s");
		return;
	}
	
	public static void main(String [] args)
	{
		AWSCredentials credentials;
		try {
			credentials = new PropertiesCredentials(PeriodicCalculator.class.getClassLoader().getResourceAsStream("AwsCredentials.properties"));
			AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials);
			client.setRegion(Region.getRegion(Regions.US_EAST_1)); 
			DynamoDBMapper mapper = new DynamoDBMapper(client);
			DynamoDBMapperConfig dynamo_config = new DynamoDBMapperConfig(DynamoDBMapperConfig.ConsistentReads.EVENTUAL);
			PeriodicCalculator pc = new PeriodicCalculator(mapper, dynamo_config, client);
			pc.start();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		
	
	}
}