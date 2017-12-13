package com.amazonaws.lambda.snsproducer;
 
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

import java.text.SimpleDateFormat;
import java.util.Date;
 
public class LambdaFunctionHandler implements RequestHandler<Object, String> {
	static final int MAX_COUNT = 10000;
	static String topicARN = "arn:aws:sns:us-west-2:983671419152:test-Delivery";
    static String TOKEN = "eee.10000.";
	static AmazonSNS  amazonSNS = null;
	static {
		amazonSNS = AmazonSNSClient.builder()
	                .withRegion(Regions.US_WEST_2)
	                .withCredentials(new DefaultAWSCredentialsProviderChain())
	                .build();
	}
    @Override
    public String handleRequest(Object input, Context context) {
    		try { 
    			context.getLogger().log(TOKEN + "handleRequest :" + context.getAwsRequestId());	
    	       //publish to an SNS topic
    	        for(int i = 0; i < MAX_COUNT; i++){
    				 String uniqueMsg = TOKEN + String.valueOf(i) + ".";
    	        		 String msg = "{\"test\":{\"token\":\"TOKEN\",\"ts\":\"TIMESTAMP\",\"p-reqId\":\"REQUESTID\",\"remain\":\"REMAIN\"},\"producer\":{\"platformEndpoint\":\"arn:aws:sns:us-west-2:123456789012:endpoint/GCM/gcmpushapp/5e3e9847-3183-3f18-a7e8-671c3a57d4b3\",\"userId\":{\"userId\":\"SYNUZAGLYCJRLYNXYDACADWe\",\"userIdType\":\"GUID\"},\"notificationId\":\"29f73551-e028-11e7-a6fb-e4115bd5403f-01021\",\"timestamp\":\"1513184876546\",\"servingLayerURL\":\"http://genesis-frontpage-yql.fp.yahoo.com:4080/v1/public/yql\",\"landingPage\":\"https://www.yahoo.com/news/m/9e053a3b-bf9d-3b44-9a21-e88ec33bafe7/ss_daughter-cashed-her-dead.html\",\"type\":\"EDITORIAL\",\"shouldStagger\":false,\"publisher\":\"Lauren Johnston\",\"OS\":{\"osName\":\"Android\",\"version\":\"8.1\"},\"app\":{\"appId\":\"905016ad-dbea-4d35-9bf2-38b63e08d3d5\",\"version\":\"4.5.0.1\"},\"payload\":{\"apns\":{\"aps\":{\"content-available\":0,\"url\":{\"id\":\"1f27c868-81aa-3f30-976e-ee7a7f185dc1\"},\"alert\":{\"body\":\"Daughter cashed her dead mom’s Social Security checks for 24 years, but now she’ll pay\"}}},\"gcm\":{\"alert\":{\"aps\":{\"content-available\":0,\"url\":{\"id\":\"1f27c868-81aa-3f30-976e-ee7a7f185dc1\"},\"alert\":{\"body\":\"Daughter cashed her dead mom’s Social Security checks for 24 years, but now she’ll pay\"}}}}}}}";
    				 msg = msg.replace("TIMESTAMP", String.valueOf(System.currentTimeMillis()));
    				 msg = msg.replace("REQUESTID", String.valueOf(context.getAwsRequestId()));
    				 msg = msg.replace("TOKEN", uniqueMsg);
    				 msg = msg.replace("REMAIN", String.valueOf(context.getRemainingTimeInMillis()));
    				 long start = System.currentTimeMillis();
    				 PublishRequest publishRequest = new PublishRequest(topicARN, msg);
    				 PublishResult publishResult = amazonSNS.publish(publishRequest);
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