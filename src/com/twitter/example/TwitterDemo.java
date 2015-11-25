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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
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
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;


public class TwitterDemo {

    static String consumerKeyStr 		= "XXXXXXXXXXXXXXXXXXXXXXXX";
	static String consumerSecretStr 	= "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
	static String accessTokenStr 		= "XXXXXX-XXXXXXXXXXXXXXXXXX";
	static String accessTokenSecretStr 	= "XXXXXXXXXXXXXXXXXXXXXXXXX";
	
	private Properties prop = null;
	private String[] languages;
	public int languageCount = 0; // keep count of the tweet found, after the count has reached the MAX_VALUE, the stream MUST be stopped and language updated
	private int tweetCount = 0; // keep count of incoming tweets, must be reset to 0 after it has reached MAX_TWEET_COUNT
	private static int MAX_TWEET_COUNT = 200; // at most 100 tweets from the stream for a language
	private final static Object lock = new Object();
	public List<String> tweetRawJSONData ;
	public JSONObject jsonObject;
	HashMap<String,String> langKeyWordPair;
	TwitterDemo()
	{
		tweetRawJSONData = new ArrayList<>();
		langKeyWordPair  = new HashMap<>();
		//KeyWords = new String[];
	}
	/**
	 * 
	 * @param fileName
	 */
	public void MyPropAllKeys(String fileName){
         try ( InputStream inputStream = new FileInputStream(fileName)) {
            this.prop = new Properties();
            //is = this.getClass().getResourceAsStream(fileName);
            
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
		/*String lang = "en,de,ru,fr,ar"; //default
		//EnglishKeyWords = "Books,Theatre,tv,television,arrow,brad pitt,angelina jolie,jolie,hollywood,bollywood,oscars";
		//TODO : Generate list of German and Russian keywords
		try ( InputStream inputStream = new FileInputStream(fileName))
		{
			
			Properties prop = new Properties();
			if ( inputStream != null)
			{
				prop.load(inputStream);
				/*if (!prop.getProperty("Language").trim().equalsIgnoreCase("")) {
					lang = prop.getProperty("Language").trim();
					
					KeyWords = new String[lang.split(",").length];
				}*/
				/*KeyWords[0] = prop.getProperty("EnglishKeyWords");
				KeyWords[1] = prop.getProperty("RussianKeyWords");
				KeyWords[2] = prop.getProperty("GermanKeyWords");
				KeyWords[3] = prop.getProperty("FrenchKeyWords");
				KeyWords[4] = prop.getProperty("ArabicKeyWords");*/
				/*if (!prop.getProperty("EnglishKeyWords").equalsIgnoreCase("")) {
					EnglishKeyWords = prop.getProperty("EnglishKeyWords");
				}
				if (!prop.getProperty("GermanKeyWords").equalsIgnoreCase("")) {
					GermanKeyWords = prop.getProperty("GermanKeyWords");
				}
				if (!prop.getProperty("RussianKeyWords").equalsIgnoreCase("")) {
					RussianKeyWords = prop.getProperty("RussianKeyWords");
				}*/				
			//}
		/*}
		catch(Exception ex)
		{
			System.out.println("Exception occured in getting properties function :" + ex.getMessage());
		}*/
		//return (lang);
	//}
	
	StatusListener listener = new StatusListener(){
	    public void onStatus(Status status) {
	        //String lang = status.getLang().toLowerCase();
	    	try
	    	{
		    	String lang = status.getLang();
		    	//System.out.println("tweet : " + status.getText());
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
		    	} else {
		    		//System.out.println("Lang :" + lang );
		    	}
	    	}
	    	catch(Exception ex)
	    	{
	    		
	    	}
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
			if (directory.mkdir()) {
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
		simpleDateFormat.applyPattern("yyyy-MM-dd'T'hh:mm:ss'Z'");
		List<String> hashtag, expandedUrlList ;// = new ArrayList<>();

		try {
			JSONArray jsonArray_hashtag, jsonArray_entity_urls;
			JSONObject jObj;
			for ( int i =0; i  < jsonArray_Temp.length(); i++ ) {
				//jObj.
				jObj = new JSONObject(jsonArray_Temp.getJSONObject(i).toString());
				jsonObject = new JSONObject();
				jsonObject.put("id", jObj.get("id"));
				jsonObject.put("lang", jObj.get("lang"));
				jsonObject.put("text_" + jObj.get("lang") , jObj.get("text"));
				jsonObject.put("created_at", simpleDateFormat.format( new Date(jObj.get("created_at").toString())   ));
				
				jsonArray_hashtag = new JSONArray(jObj.getJSONObject("entities").getJSONArray("hashtags").toString());
				hashtag = new ArrayList<>();
				for ( int hashtagCounter = 0; hashtagCounter < jsonArray_hashtag.length() ; hashtagCounter++ ) {
					hashtag.add(jsonArray_hashtag.getJSONObject(hashtagCounter).get("text").toString());
				}
				jsonObject.put("tweet_hashtag", hashtag.toString());
				
				jsonArray_entity_urls = new JSONArray(jObj.getJSONObject("entities").getJSONArray("urls").toString());
				expandedUrlList = new ArrayList<>();
				for ( int entityUrlCount = 0; entityUrlCount < jsonArray_entity_urls.length() ; entityUrlCount++ ) {
					expandedUrlList.add(jsonArray_entity_urls.getJSONObject(entityUrlCount).get("expanded_url").toString());
				}
				jsonObject.put("tweet_urls",expandedUrlList.toString());
				
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
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy_MM_dd");
		directory = languages[languageCount] + "//"  + simpleDateFormat.format(new Date());
		CreateDirectory(directory);
		//tweetRawJSONData.add(convertJSONRawTorequiredFormat());
	    try {
	    	File file = new File(directory + "//" +   fileNameWithTweetData);
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

	public void extractRelevantJSONFields(String rawJSONData)
	{
		
	}
	public static void main(String[] args) throws TwitterException {

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
			/*switch(tweetDemo.languages[tweetDemo.languageCount])
			{
			case "en":
				tweetKeyWords = tweetDemo.EnglishKeyWords.split(",");
				break;
			case "de":
				tweetKeyWords = tweetDemo.GermanKeyWords.split(",");
				break;
			case "ru":
				tweetKeyWords = tweetDemo.RussianKeyWords.split(",");
				break;			
			default:
				tweetKeyWords = tweetDemo.EnglishKeyWords.split(",");
				break;
			}*/
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