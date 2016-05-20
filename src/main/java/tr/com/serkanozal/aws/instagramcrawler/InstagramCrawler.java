package tr.com.serkanozal.aws.instagramcrawler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.jinstagram.auth.InstagramAuthService;
import org.jinstagram.auth.oauth.InstagramService;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.identitymanagement.model.AttachRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyResult;
import com.amazonaws.services.identitymanagement.model.CreateRoleRequest;
import com.amazonaws.services.identitymanagement.model.GetPolicyRequest;
import com.amazonaws.services.identitymanagement.model.GetRoleRequest;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.BufferingHints;
import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.ResourceNotFoundException;
import com.amazonaws.services.kinesisfirehose.model.S3DestinationConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;

public class InstagramCrawler {

    private static final Logger LOGGER = Logger.getLogger(InstagramCrawler.class);
    
    private static final AWSCredentials awsCredentials;
    
    static {
    	try {
	    	// Twitter stream authentication setup
	        Properties awsProps = getProperties("aws-credentials.properties");
	
	        awsCredentials = 
	        		new BasicAWSCredentials(
	        				awsProps.getProperty("aws.accessKey"), 
	        				awsProps.getProperty("aws.secretKey"));
    	} catch (IOException e) {
    		throw new RuntimeException(e);
    	}
    }
    
    public static void main(String[] args) throws Exception {
        String streamName = createInstagramStream();
        
        startInstagramCrawling(streamName);
    }
    
    private static Properties getProperties(String propFileName) throws IOException {
    	Properties props = new Properties();
        try {
            InputStream in = InstagramCrawler.class.getClassLoader().getResourceAsStream(propFileName);
            if (in != null) {
            	props.load(in);
            } 
            props.putAll(System.getProperties());
            return props;
        } catch (IOException e) {
            LOGGER.error("Error occured while loading properties from " + 
            			 "'" + propFileName + "'", e);
            throw e;
        }
    }
    
    private static String createInstagramStream() throws IOException {
        // Twitter stream configuration setup
        Properties streamProps = getProperties("instagram-stream.properties");

        String streamName = streamProps.getProperty("aws.instagram.streamName");
        String bucketName = streamProps.getProperty("aws.instagram.bucketName");
        Integer destinationSizeInMBs = Integer.parseInt(streamProps.getProperty("aws.instagram.bufferSize", "64")); // Default 64 MB
        Integer destinationIntervalInSeconds = Integer.parseInt(streamProps.getProperty("aws.instagram.bufferTime", "600")); // Default 10 mins
        String accountId = null;
        
        ////////////////////////////////////////////////////////////////
        
        AmazonIdentityManagementClient iamClient = new AmazonIdentityManagementClient(awsCredentials);
        AmazonS3Client s3Client = new AmazonS3Client(awsCredentials);
        AmazonKinesisFirehoseClient firehoseClient = new AmazonKinesisFirehoseClient(awsCredentials);
        
        ////////////////////////////////////////////////////////////////

        accountId = iamClient.getUser().getUser().getUserId();
        
        String roleName = "IAM_Role_for_" + streamName;
        boolean skipRoleCreation = false;
        try {
            iamClient.getRole(new GetRoleRequest().withRoleName(roleName));
            skipRoleCreation = true;
        } catch (NoSuchEntityException e) {
        }
        
        if (!skipRoleCreation) {
            String roleDefinition = null; 
            InputStream inRole = InstagramCrawler.class.getClassLoader().getResourceAsStream("role-definition-template");
            Scanner scanRole = new Scanner(inRole);  
            scanRole.useDelimiter("\\Z");  
            roleDefinition = scanRole.next(); 
    
            CreateRoleRequest createRoleRequest = new CreateRoleRequest();
            createRoleRequest.setRoleName(roleName);
            createRoleRequest.setPath("/");
            createRoleRequest.setAssumeRolePolicyDocument(roleDefinition);
            iamClient.createRole(createRoleRequest);
        }
        
        ////////////////////////////////////////////////////////////////
        
        String policyName = "Policy_for_" + roleName;
        String policyArn = "arn:aws:iam::" + accountId + ":policy/" + policyName;
        
        boolean skipPolicyCreation = false;
        try {
            iamClient.getPolicy(new GetPolicyRequest().withPolicyArn(policyArn));
            skipPolicyCreation = true;
        } catch (NoSuchEntityException e) {
        }
        
        if (!skipPolicyCreation) {
            String policyDefinition = null; 
            InputStream inPolicy = InstagramCrawler.class.getClassLoader().getResourceAsStream("policy-definition-template");
            Scanner scanPolicy = new Scanner(inPolicy);  
            scanPolicy.useDelimiter("\\Z");  
            policyDefinition = scanPolicy.next(); 
            
            policyDefinition = policyDefinition.replace("${aws.instagram.bucketName}", bucketName);
            policyDefinition = policyDefinition.replace("${aws.instagram.streamName}", streamName);
            
            CreatePolicyRequest createPolicyRequest = new CreatePolicyRequest();
            createPolicyRequest.setPolicyName(policyName);
            createPolicyRequest.setPolicyDocument(policyDefinition);
            CreatePolicyResult createPolicyResult = iamClient.createPolicy(createPolicyRequest);
            
            AttachRolePolicyRequest attachRolePolicyRequest = new AttachRolePolicyRequest();
            attachRolePolicyRequest.setRoleName(roleName);
            attachRolePolicyRequest.setPolicyArn(createPolicyResult.getPolicy().getArn());
            iamClient.attachRolePolicy(attachRolePolicyRequest);
            
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
        }
        
        ////////////////////////////////////////////////////////////////

        if (!s3Client.doesBucketExist(bucketName)) {
            s3Client.createBucket(bucketName);
        }
        
        ////////////////////////////////////////////////////////////////

        boolean skipStreamCreation = false;
        try {
            firehoseClient.describeDeliveryStream(new DescribeDeliveryStreamRequest().withDeliveryStreamName(streamName));
            skipStreamCreation = true;
        } catch (ResourceNotFoundException e) {
        }
        
        if (!skipStreamCreation) {
            // Create deliveryStream
            CreateDeliveryStreamRequest createDeliveryStreamRequest = new CreateDeliveryStreamRequest();
            createDeliveryStreamRequest.setDeliveryStreamName(streamName);
    
            S3DestinationConfiguration s3DestinationConfiguration = new S3DestinationConfiguration();
            s3DestinationConfiguration.setBucketARN("arn:aws:s3:::" + bucketName);
            // Could also specify GZIP, ZIP, or SNAPPY
            s3DestinationConfiguration.setCompressionFormat(CompressionFormat.UNCOMPRESSED);
    
            BufferingHints bufferingHints = null;
            if (destinationSizeInMBs != null || destinationIntervalInSeconds != null) {
                bufferingHints = new BufferingHints();
                bufferingHints.setSizeInMBs(destinationSizeInMBs);
                bufferingHints.setIntervalInSeconds(destinationIntervalInSeconds);
            }
            s3DestinationConfiguration.setBufferingHints(bufferingHints);
    
            // Create and set the IAM role so that Firehose has access to the S3 buckets to put data
            // and AWS KMS keys (if provided) to encrypt data. Please check the trustPolicyDocument.json and
            // permissionsPolicyDocument.json files for the trust and permissions policies set for the role.
            s3DestinationConfiguration.setRoleARN("arn:aws:iam::" + accountId + ":role/" + roleName);
    
            createDeliveryStreamRequest.setS3DestinationConfiguration(s3DestinationConfiguration);
    
            firehoseClient.createDeliveryStream(createDeliveryStreamRequest);
        }
        
        ////////////////////////////////////////////////////////////////

        return streamName;
    }
    
    private static void startInstagramCrawling(String streamName) throws Exception {
        AmazonKinesisFirehoseClient firehoseClient = new AmazonKinesisFirehoseClient(awsCredentials);
        DescribeDeliveryStreamRequest describeDeliveryStreamRequest = new DescribeDeliveryStreamRequest();
        describeDeliveryStreamRequest.setDeliveryStreamName(streamName);
        
        while (true) {
            DescribeDeliveryStreamResult deliveryStreamResult = 
                    firehoseClient.describeDeliveryStream(describeDeliveryStreamRequest);
            if ("ACTIVE".equals(deliveryStreamResult.getDeliveryStreamDescription().getDeliveryStreamStatus())) {
                break;
            }
            LOGGER.info("Waiting stream " + streamName + " to be activated ...");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }   
        
        ////////////////////////////////////////////////////////////////
        
        // Twitter stream authentication configurations
        Properties instagramProps = getProperties("instagram-credentials.properties");
        
        /*
         * See
         *      - https://github.com/sachin-handiekar/jInstagram
         *      - https://github.com/sachin-handiekar/jInstagram/wiki/Instagram-Realtime-API
         *      - https://github.com/sachin-handiekar/jInstagram-examples
         */
        String clientId = instagramProps.getProperty("instagram.client.id");
        String clientSecret = instagramProps.getProperty("instagram.client.secret");
        String callbackUrl = "http://localhost:8080/AppName/handleInstagramToken/";

        InstagramService service = 
                new InstagramAuthService()
                    .apiKey(clientId)
                    .apiSecret(clientSecret)
                    .callback(callbackUrl)
                    .build();
        
        System.out.println(service.getAuthorizationUrl());
        
        Server server = new Server(8080);
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, 
                               Request baseRequest, 
                               HttpServletRequest request, 
                               HttpServletResponse response) throws IOException, ServletException {
                response.setContentType("text/html;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);
                baseRequest.setHandled(true);
                response.getWriter().println("OK");
            }
        });
        server.start();
        server.join();
    }    
    
}
