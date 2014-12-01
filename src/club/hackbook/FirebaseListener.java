package club.hackbook;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
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
 				  
 				
 				 // If I own the firebase lock OR the firebase lock is more than 2.5 minutes old, take it over.
 				 long now = System.currentTimeMillis();
 				 //boolean ihavethepower = true;
 				 
 				 boolean ihavethepower = false;
 				 firebase_owner_id_gvi = mapper.load(GlobalvarItem.class, "firebase_owner_id", dynamo_config); // we can assume this is never null
 				 firebase_last_msfe_gvi = mapper.load(GlobalvarItem.class, "firebase_last_msfe", dynamo_config); // we can assume this is never null
				 if(!firebase_owner_id_gvi.getStringValue().equals(myId))
				 {
					 if(firebase_last_msfe_gvi.getNumberValue() > System.currentTimeMillis()-150000) 
					 {
						 System.out.println("*** I do NOT have the power2! *** myId=" + myId + " owner=" + firebase_owner_id_gvi.getStringValue() + " firebase_last_msfe is " + (System.currentTimeMillis()-firebase_last_msfe_gvi.getNumberValue()) + " old.");
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
					 System.out.println("Inside if(ihavethepower==true) myId=" + myId);
					 FirebaseChangeProcessor fcp = new FirebaseChangeProcessor(snapshot, mapper, dynamo_config, client);
	 				 fcp.start();
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
    
}