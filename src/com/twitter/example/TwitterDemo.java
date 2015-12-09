package com.twitter.example;

/*
 * Twitter Retriever project created to search tweets on trending topics in multiple languages
 * 
 * Several URLs have been referred to in creating the project , namely some of them are :
 * -- Google group of Twitter4j API
 * -- http://twitter4j.org/en/code-examples.html
 * -- Twitter4j code examples
 * -- http://thinktostart.com/build-your-own-twitter-archive-and-analyzing-infrastructure-with-mongodb-java-and-r-part-1/
 * -- http://crunchify.com/java-properties-file-how-to-read-config-properties-values-in-java/
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import twitter4j.FilterQuery;
import twitter4j.JSONArray;
import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;


public class TwitterDemo {

    static String consumerKeyStr 		= "WZSv8sHDBn73hexm27uYKQAgk";
	static String consumerSecretStr 	= "pKpnJdH5oBTHsCGZJb71wVv7cOEbYoHi1WdJp9nJ969tXBFAFC";
	static String accessTokenStr 		= "2959803912-yh9Hx6xh95UBF4IvkwQzXSWuIIVD5iGvMX9xN9Y";
	static String accessTokenSecretStr 	= "50IwxSPSONSBHO4jdxC7P6k1zYvMmMLLj7bRlband7RsE";
	
	private Properties prop = null;
	private String[] languages;
	public int languageCount = 0; // keep count of the tweet found, after the count has reached the MAX_VALUE, the stream MUST be stopped and language updated
	private int tweetCount = 0; // keep count of incoming tweets, must be reset to 0 after it has reached MAX_TWEET_COUNT
	private static int MAX_TWEET_COUNT = 200; // at most 200 tweets from the stream for a language
	private final static Object lock = new Object();
	public List<String> tweetRawJSONData ;
	public JSONObject jsonObject;
	HashMap<String,String> langKeyWordPair;
	TwitterDemo()
	{
		tweetRawJSONData = new ArrayList<>();
		langKeyWordPair  = new HashMap<>();
	}
	/**
	 * 
	 * @param fileName
	 */
	public void MyPropAllKeys(String fileName){
         try ( InputStream inputStream = new FileInputStream(fileName)) {
            this.prop = new Properties();
            prop.load(inputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	/**
	 *      
	 * @return
	 */
    public Set<Object> getAllKeys(){
        Set<Object> keys = prop.keySet();
        return keys;
    }
     
    public String getPropertyValue(String key){
        return this.prop.getProperty(key);
    }
	/**
	 * 
	 * @param fileName
	 */
	public void getPropertiesData(String fileName)
	{
		MyPropAllKeys(fileName);
		Set<Object> keys = getAllKeys();
		languages = new String[keys.size()];
		int nCount = 0; 
        for(Object k:keys){
            String key = (String)k;
            languages[nCount] = key;
            langKeyWordPair.put(key, getPropertyValue(key));
            System.out.println(key+": "+ getPropertyValue(key));
            nCount++;
        }
	}   
	
	StatusListener listener = new StatusListener(){
	    public void onStatus(Status status) {
	    	try
	    	{
		    	String lang = status.getLang();
	    		if  (  languages[languageCount].equalsIgnoreCase(lang)  )
		    	{
			    	if ( !status.getText().trim().equalsIgnoreCase("")) // only to index those tweets which has some text in it
			    	{
			    		tweetRawJSONData.add(TwitterObjectFactory.getRawJSON(status));
			    		tweetCount++;
			    		if ( tweetCount >= MAX_TWEET_COUNT) {
			    			synchronized (lock) {
			    				lock.notify();
							}
			    		}
			    		System.out.println(status.getText() + "\nLanguage :" + status.getLang().toString() + "\nCount :" + tweetCount + "\n****************\n");
			    	}
		    	}
	    	}
	    	catch(Exception ex) {  	}
	    }
	    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
	    	
	    }
	    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
	    	
	    }
	    public void onException(Exception ex) {
	        ex.printStackTrace();
	    }
		public void onScrubGeo(long arg0, long arg1) {
			// TODO Auto-generated method stub
			
		}
		public void onStallWarning(StallWarning arg0) {
			// TODO Auto-generated method stub
			
		}
	};
	
	public boolean CreateDirectory(String DirectoryName )
	{
		File directory = new File(DirectoryName);
		if (!directory.exists()) {
			if (directory.mkdirs()) {
				return true;
			} else {
				return false;
			}
		}
		else {
			return true;
		}
	}
	public String convertJSONRawTorequiredFormat() {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy_MM_dd");
		JSONArray jsonArray = new JSONArray();
		JSONArray jsonArray_Temp = null;
		JSONObject jsonObject = new JSONObject();
		try {
			jsonArray_Temp = new JSONArray(tweetRawJSONData.toString());
			tweetRawJSONData.clear();
		} catch (JSONException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		ArrayList<String> langList = new ArrayList<>();
		langList.add("ar");
		langList.add("de");
		langList.add("en");
		langList.add("fr");		
		langList.add("ru");
		
		simpleDateFormat.applyPattern("yyyy-MM-dd'T'hh:mm:ss'Z'");
		List<String> hashtag, expandedUrlList, displayUrlList, urlList , userMentions_ScreenName, userMentions_name, userMentions_id;
		
		try {
			JSONArray jsonArray_hashtag, jsonArray_entity_urls, jsonArray_userMentions;
			JSONObject jsonObj_Users, jsonObj_extended_entities_media, jObj;
			for ( int i =0; i  < jsonArray_Temp.length(); i++ ) {
				jObj = new JSONObject(jsonArray_Temp.getJSONObject(i).toString());
				jsonObject = new JSONObject();
				jsonObject.put("id", jObj.get("id"));
				jsonObject.put("lang", jObj.get("lang"));
				try {
					jsonObject.put("source", jObj.get("source"));
				}
				catch(Exception ex) {
					System.out.println("Source field not found ! Skipping...");
				}
				try {
					jsonObject.put("favorited", jObj.get("favorited"));
				} catch (Exception e) {
					System.out.println("Favorited field not found ! Skipping...");
				}				
				jsonObject.put("text_" + jObj.get("lang") , jObj.get("text"));
				for  (String s : langList) {
					if ( !(jObj.get("lang").toString().equals(s)) ) {
						jsonObject.put("text_" + s , "");
					}
				}
				jsonObject.put("created_at", simpleDateFormat.format( new Date(jObj.get("created_at").toString())   ));
				try {
					if ( jObj.getJSONObject("entities") != null ) {
						jsonArray_hashtag = new JSONArray(jObj.getJSONObject("entities").getJSONArray("hashtags").toString());
						hashtag = new ArrayList<>();
						for ( int hashtagCounter = 0; hashtagCounter < jsonArray_hashtag.length() ; 		hashtagCounter++ ) {
							hashtag.add(jsonArray_hashtag.getJSONObject(hashtagCounter).get("text").toString());
						}
						jsonObject.put("entities_tweet_hashtags", hashtag.toString());
					
						jsonArray_entity_urls = new JSONArray(jObj.getJSONObject("entities").getJSONArray("urls").toString());
						expandedUrlList = new ArrayList<>();
						displayUrlList  = new ArrayList<>();
						urlList = new ArrayList<>();
						for ( int k = 0; k < jsonArray_entity_urls.length() ; k++ ) {
							expandedUrlList.add(jsonArray_entity_urls.getJSONObject(k).get("expanded_url").toString());
							displayUrlList.add(jsonArray_entity_urls.getJSONObject(k).get("display_url").toString());
							urlList.add(jsonArray_entity_urls.getJSONObject(k).get("url").toString());
						}
						
						
						jsonObject.put("entities_tweet_expandedUrl",expandedUrlList.toString());
						jsonObject.put("entities_tweet_urls",expandedUrlList.toString());
						jsonObject.put("entities_tweet_displayUrl",expandedUrlList.toString());
					}
				} catch (Exception e) {
					System.out.println("Entities field not found ! Skipping...");
				}	
				//extended entities array
				try {
					if ( jObj.getJSONObject("extended_entities") != null) {
						jsonObj_extended_entities_media = new JSONObject(jObj.getJSONObject("extended_entities").getJSONArray("media").get(0).toString());
						jsonObject.put("entities_tweet_media_url", jsonObj_extended_entities_media.get("media_url"));
					}
				}
				catch(Exception ex) {
					System.out.println("Extended_entities not found. Skipping....");
				}
				//user_mentions array
				//if (jObj.getJSONObject("entities") )
				userMentions_ScreenName = new ArrayList<>();
				userMentions_name       = new ArrayList<>();
				userMentions_id         = new ArrayList<>();
				try {
					jsonArray_userMentions = new JSONArray(jObj.getJSONObject("entities").getJSONArray("user_mentions").toString());
					for (int k = 0 ; k < jsonArray_userMentions.length(); k++) {
						userMentions_ScreenName.add(jsonArray_userMentions.getJSONObject(k).get("screen_name").toString());
						userMentions_name.add(jsonArray_userMentions.getJSONObject(k).get("name").toString());
						userMentions_id.add(jsonArray_userMentions.getJSONObject(k).get("id").toString());
						
					}
					jsonObject.put("entities_user_mentions_screen_name",userMentions_ScreenName.toString());
					jsonObject.put("entities_user_mentions_name",userMentions_name.toString());
					jsonObject.put("entities_user_mentions_id",userMentions_id.toString());
				}
				catch(Exception ex) {
					System.out.println("Exception occured while fetching user_mentions ! Skipping...");
				}
				//users information array
				try {
					jsonObject.put("users_followers_count", jObj.getJSONObject("user").get("followers_count"));
					jsonObject.put("users_screen_name", jObj.getJSONObject("user").get("screen_name"));
					jsonObject.put("users_verified", jObj.getJSONObject("user").get("verified"));
					jsonObject.put("users_lang", jObj.getJSONObject("user").get("lang"));
					jsonObject.put("users_profile_image_url", jObj.getJSONObject("user").get("profile_image_url"));
				} catch (Exception e) {
					System.out.println("Users information not found ! Skipping...");
				}
				
				jsonArray.put(jsonObject);
		} 
		}catch (JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return jsonArray.toString();
		
		
	}
	@SuppressWarnings("deprecation")
	public void WriteTweetDataInUTF8(String fileNameWithTweetData, List<String> tweetRawJSONData) {
		
		String directory ;
		List<String> tweetFinalJSONData = new ArrayList<>();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss");
		directory = "Tweets\\" + languages[languageCount] + "\\"  + simpleDateFormat.format(new Date());
		if (!CreateDirectory(directory)) {
			System.out.println(directory  +" not created");
		}
		
	    try {
	    	File file = new File(directory +  "//" + fileNameWithTweetData);
	    System.out.println(directory + "//" + fileNameWithTweetData);	
	    	if (!file.exists()) {
	    		file.createNewFile();
	    	}
	    	OutputStream outputStream       = new FileOutputStream(file);
	    	Writer       outputStreamWriter = new OutputStreamWriter(outputStream);
	    
	    	
	    	outputStreamWriter.write(tweetRawJSONData.toString());

	    	outputStreamWriter.close();
	    	System.out.println("Created ffile is =" + new File(fileNameWithTweetData).getAbsolutePath());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
]	public static void main(String[] args) throws TwitterException {

		TwitterDemo tweetDemo = new TwitterDemo();
		String tweetKeyWords[] = null;
		tweetDemo.getPropertiesData((new File("")).getAbsolutePath()  + "\\src\\com\\twitter\\example\\" + "DefaultProperties.properties");
		//int totalCount = (tweetDemo.langKeyWordPair.size());
		tweetDemo.languageCount = 0;
		//tweetDemo.KeyWords = new String[tweetDemo.languageCount];
		tweetDemo.jsonObject = new JSONObject();
		tweetKeyWords = new String[1];
		while ( tweetDemo.languageCount < tweetDemo.langKeyWordPair.size())
		{
			tweetDemo.tweetCount = 0;
			ConfigurationBuilder cb = new ConfigurationBuilder();
			cb.setDebugEnabled(true);
			cb.setOAuthConsumerKey(consumerKeyStr);
			cb.setOAuthConsumerSecret(consumerSecretStr);
			cb.setOAuthAccessToken(accessTokenStr);
			cb.setOAuthAccessTokenSecret(accessTokenSecretStr);
			cb.setJSONStoreEnabled(true); // We will retrieve the tweets in Raw JSON format, since SOLR supports native JSON indexing as is

			TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();			
		/*
		 * StatusListener is a streaming API 
		 * Active language is denoted by counter languageCount
		*/ 
	
			tweetDemo.tweetRawJSONData.clear();
			FilterQuery fq = new FilterQuery();
			//tweetDemo.languages = tweetFilters.split(",");
			tweetKeyWords = tweetDemo.langKeyWordPair.get(tweetDemo.languages[tweetDemo.languageCount]).split(",");
			// DONT use both language and keywords, as these are logical OR, not logical AND
			fq.track(tweetKeyWords);
			twitterStream.addListener(tweetDemo.listener);
			twitterStream.filter(fq);
			synchronized (lock) {
				try {
					lock.wait();
					
					// Now we have some tweets in JSON format in tweetRawJSONData, we will save it in a text file with UTF-8 encoding ( with BOM )
					// This file will be named as "Tweets_<languageName>_<Date>.txt
					// Each language must get 100 tweets / day minimum. 
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			tweetDemo.CreateDirectory(tweetDemo.languages[tweetDemo.languageCount]);
			tweetDemo.WriteTweetDataInUTF8("TwitterText_" + tweetDemo.languages[tweetDemo.languageCount] + ".txt", tweetDemo.tweetRawJSONData);
			tweetDemo.languageCount++;
			System.out.println("Successfully updated the status in Twitter.");
			//twitterStream.removeListener(tweetDemo.listener);
			try {
				twitterStream.shutdown();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		
		
	}

}