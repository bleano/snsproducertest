package com.amazonaws.lambda.snsproducer;
 
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
 
public class LambdaFunctionHandler implements RequestHandler<Object, String> {
	private static final int MAX_COUNT = 5000;
	String topicARN = "arn:aws:sns:us-west-2:983671419152:test-Delivery";
	public static String TOKEN = "zsz.5000.";
 
    @Override
    public String handleRequest(Object input, Context context) {
    		try {
    			ClientConfiguration clientConfig = new ClientConfiguration();
    			clientConfig.setClientExecutionTimeout(30000);
    	        AmazonSNSClient  snsClient  = (AmazonSNSClient) AmazonSNSClientBuilder.standard()
    	                .withRegion(Regions.US_WEST_2).withClientConfiguration(clientConfig).build();  

    	       //publish to an SNS topic
    	        for(int i = 0; i < MAX_COUNT; i++){
    				 String uniqueMsg = TOKEN + String.valueOf(i) + ".";
    	        		 String msg = "{\"evt:\":\"EVENTNUM\",\"ts\":TIMESTAMP,\"p-reqId\":\"REQUESTID\", \"remain\":\"REMAIN\"}";
    				 msg = msg.replace("TIMESTAMP", String.valueOf(System.currentTimeMillis()));
    				 msg = msg.replace("REQUESTID", String.valueOf(context.getAwsRequestId()));
    				 msg = msg.replace("EVENTNUM", uniqueMsg);
    				 msg = msg.replace("REMAIN", String.valueOf(context.getRemainingTimeInMillis()));
    				 long start = System.currentTimeMillis();
    				 PublishRequest publishRequest = new PublishRequest(topicARN, msg);
    				 PublishResult publishResult = snsClient.publish(publishRequest);
    				 long stop = System.currentTimeMillis();
    				 context.getLogger().log("snsRes:" +  publishResult.getMessageId() + " msg: " + msg + " snsTime:" + String.valueOf(stop-start));  
    	        }
    		}catch(com.amazonaws.services.lambda.model.TooManyRequestsException e)
    		{
    	        context.getLogger().log(TOKEN + "handleRequestException:" + e.getMessage());	
    		}
 
       return "OK";
    }
   
    private String getTimestamp() {
       String pattern = "HH:mm:ss.SSSS";
       SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
       return simpleDateFormat.format(new Date());
    }
 
}