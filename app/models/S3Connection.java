package models;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TransactionConflictException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;

import netscape.javascript.JSObject;
import play.Logger;
import scala.annotation.meta.field;

@Singleton
public class S3Connection {
    private Config config;
    private String bucketSuffix = null;
    private AmazonS3 amazonS3;
    private AmazonDynamoDB amazonDynamoDB;
    private static Logger.ALogger logger = Logger.of("com.horstmann.codecheck");

    public static class OutOfOrderException extends RuntimeException {
    }

    @Inject
    public S3Connection(Config config) {
        this.config = config;
        // For local testing only--TODO: What exactly should work in this situation?
        if (!config.hasPath("com.horstmann.codecheck.s3.accessKey"))
            return;

        String s3AccessKey = config.getString("com.horstmann.codecheck.s3.accessKey");
        String s3SecretKey = config.getString("com.horstmann.codecheck.s3.secretKey");
        String s3Region = config.getString("com.horstmann.codecheck.s3.region");
        amazonS3 = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(s3AccessKey, s3SecretKey)))
                .withRegion(s3Region)
                .withForceGlobalBucketAccessEnabled(true)
                .build();

        amazonDynamoDB = AmazonDynamoDBClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(s3AccessKey, s3SecretKey)))
                .withRegion("us-west-1")
                .build();

        bucketSuffix = config.getString("com.horstmann.codecheck.s3bucketsuffix");
    }

    public boolean isOnS3(String repo) {
        String key = "com.horstmann.codecheck.repo." + repo;
        return !config.hasPath(key) || config.getString(key).isEmpty();
    }

    public boolean isOnS3(String repo, String key) {
        String bucket = repo + "." + bucketSuffix;
        return getS3Connection().doesObjectExist(bucket, key);
    }

    private AmazonS3 getS3Connection() {
        return amazonS3;
    }

    public AmazonDynamoDB getAmazonDynamoDB() {
        return amazonDynamoDB;
    }

    public void putToS3(Path file, String repo, String key)
            throws IOException {
        String bucket = repo + "." + bucketSuffix;
        try {
            getS3Connection().putObject(bucket, key, file.toFile());
        } catch (AmazonS3Exception ex) {
            logger.error("S3Connection.putToS3: Cannot put " + file + " to " + bucket);
            throw ex;
        }
    }

    public void putToS3(String contents, String repo, String key)
            throws IOException {
        String bucket = repo + "." + bucketSuffix;
        try {
            getS3Connection().putObject(bucket, key, contents);
        } catch (AmazonS3Exception ex) {
            logger.error("S3Connection.putToS3: Cannot put "
                    + contents.replaceAll("\n", "|").substring(0, Math.min(50, contents.length())) + "... to "
                    + bucket);
            throw ex;
        }
    }

    public void putToS3(byte[] contents, String repo, String key)
            throws IOException {
        String bucket = repo + "." + bucketSuffix;
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contents.length);
        metadata.setContentType("application/zip");
        try {
            try (ByteArrayInputStream in = new ByteArrayInputStream(contents)) {
                getS3Connection().putObject(bucket, key, in, metadata);
            }
        } catch (AmazonS3Exception ex) {
            String bytes = Arrays.toString(contents);
            logger.error("S3Connection.putToS3: Cannot put " + bytes.substring(0, Math.min(50, bytes.length()))
                    + "... to " + bucket);
            throw ex;
        }
    }

    public void deleteFromS3(String repo, String key)
            throws IOException {
        String bucket = repo + "." + bucketSuffix;
        try {
            getS3Connection().deleteObject(bucket, key);
        } catch (AmazonS3Exception ex) {
            logger.error("S3Connection.deleteFromS3: Cannot delete " + bucket);
            throw ex;
        }
    }

    public byte[] readFromS3(String repo, String problem)
            throws IOException {
        String bucket = repo + "." + bucketSuffix;

        byte[] bytes = null;
        try {
            // TODO -- trying to avoid warning
            // WARN - com.amazonaws.services.s3.internal.S3AbortableInputStream - Not all
            // bytes were read from the S3ObjectInputStream, aborting HTTP connection. This
            // is likely an error and may result in sub-optimal behavior. Request only the
            // bytes you need via a ranged GET or drain the input stream after use
            try (InputStream in = getS3Connection().getObject(bucket, problem).getObjectContent()) {
                bytes = in.readAllBytes();
            }
        } catch (AmazonS3Exception ex) {
            logger.error("S3Connection.readFromS3: Cannot read " + problem + " from " + bucket);
            throw ex;
        }
        return bytes;
    }

    public List<String> readS3keys(String repo, String keyPrefix) throws AmazonServiceException {
        // https://docs.aws.amazon.com/AmazonS3/latest/dev/ListingObjectKeysUsingJava.html
        String bucket = repo + "." + bucketSuffix;
        ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(bucket).withMaxKeys(100).withPrefix(keyPrefix);
        ListObjectsV2Result result;
        List<String> allKeys = new ArrayList<String>();

        do {
            result = getS3Connection().listObjectsV2(req);

            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                allKeys.add(objectSummary.getKey());
            }

            String token = result.getNextContinuationToken();
            req.setContinuationToken(token);
        } while (result.isTruncated());
        return allKeys;
    }

    public ObjectNode readJsonObjectFromDynamoDB(String tableName, String primaryKeyName, String primaryKeyValue)
            throws IOException {
        String result = readJsonStringFromDynamoDB(tableName, primaryKeyName, primaryKeyValue);
        return result == null ? null : (ObjectNode) (new ObjectMapper().readTree(result));
    }

    public String readJsonStringFromDynamoDB(String tableName, String primaryKeyName, String primaryKeyValue)
            throws IOException {
        DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
        Table table = dynamoDB.getTable(tableName);
        ItemCollection<QueryOutcome> items = table.query(primaryKeyName, primaryKeyValue);
        try {
            Iterator<Item> iterator = items.iterator();
            if (iterator.hasNext())
                return iterator.next().toJSON();
            else
                return null;
        } catch (ResourceNotFoundException ex) {
            return null;
        }
    }

    public ObjectNode readJsonObjectFromDynamoDB(String tableName, String primaryKeyName, String primaryKeyValue,
            String sortKeyName, String sortKeyValue) throws IOException {
        String result = readJsonStringFromDynamoDB(tableName, primaryKeyName, primaryKeyValue, sortKeyName,
                sortKeyValue);
        return result == null ? null : (ObjectNode) (new ObjectMapper().readTree(result));
    }

    public ObjectNode readNewestJsonObjectFromDynamoDB(String tableName, String primaryKeyName,
            String primaryKeyValue) {
        DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
        Table table = dynamoDB.getTable(tableName);
        QuerySpec spec = new QuerySpec()
                .withKeyConditionExpression(primaryKeyName + " = :primaryKey")
                .withValueMap(new ValueMap().withString(":primaryKey", primaryKeyValue))
                .withScanIndexForward(false);

        ItemCollection<QueryOutcome> items = table.query(spec);
        try {
            Iterator<Item> iterator = items.iterator();
            if (iterator.hasNext()) {
                String result = iterator.next().toJSON();
                try {
                    return (ObjectNode) (new ObjectMapper().readTree(result));
                } catch (JsonProcessingException ex) {
                    return null;
                }
            } else
                return null;
        } catch (ResourceNotFoundException ex) {
            return null;
        }
    }

    public String readJsonStringFromDynamoDB(String tableName, String primaryKeyName, String primaryKeyValue,
            String sortKeyName, String sortKeyValue) throws IOException {
        DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
        Table table = dynamoDB.getTable(tableName);
        ItemCollection<QueryOutcome> items = table.query(primaryKeyName, primaryKeyValue,
                new RangeKeyCondition(sortKeyName).eq(sortKeyValue));
        try {
            Iterator<Item> iterator = items.iterator();
            if (iterator.hasNext())
                return iterator.next().toJSON();
            else
                return null;
        } catch (ResourceNotFoundException ex) {
            return null;
        }
    }

    public Map<String, ObjectNode> readJsonObjectsFromDynamoDB(String tableName, String primaryKeyName,
            String primaryKeyValue, String sortKeyName) throws IOException {
        DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
        Table table = dynamoDB.getTable(tableName);
        ItemCollection<QueryOutcome> items = table.query(primaryKeyName, primaryKeyValue);
        Iterator<Item> iterator = items.iterator();
        Map<String, ObjectNode> itemMap = new HashMap<>();
        while (iterator.hasNext()) {
            Item item = iterator.next();
            String key = item.getString(sortKeyName);
            itemMap.put(key, (ObjectNode) (new ObjectMapper().readTree(item.toJSON())));
        }
        return itemMap;
    }

    public void writeJsonObjectToDynamoDB(String tableName, ObjectNode obj) {
        if(amazonDynamoDB == null){
            writeJsonToFile(tableName, obj);
        } else{
            DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
            Table table = dynamoDB.getTable(tableName);
            table.putItem(
                    new PutItemSpec()
                            .withItem(Item.fromJSON(obj.toString())));
        }
    }

    private void writeJsonToFile(String tableName, ObjectNode obj) {

        // Create a directory for the given table
        // E.g. if the key "com.horstmann.codecheck.db" has the value
        // "/opt/codecheck/db", then the directory created should be
        final String configVal = config.getString("com.horstmann.codecheck.db");

        Path base = Path.of(configVal);
        Path child = base.resolve(tableName);
        try {
            Files.createDirectory(child); // Should create a directory with the path
                                          // /opt/codecheck/db/CodeCheckAssignments
        } catch (IOException ex) {
            logger.warn("Table directory could not be generated");
        }

        switch (tableName) {
            case "CodeCheckAssignments": // primary key == assignmentID
                try {
                    String assignmentID = obj.get("assignmentID").asText();
                    Path assignment = child.resolve(assignmentID); // should be in the format
                                                                   // /opt/codecheck/db/CodeCheckAssignments/123456,
                                                                   // where assignmentID = 123456
                    Files.writeString(assignment, obj.toString());

                    break;
                } catch (IOException ex) {
                    logger.warn("AssignmentID not found.");
                }
            case "CodeCheckLTICredentials": // primary key == oauth_consumer_key
                try {
                    String oauthConsumerKey = obj.get("oauth_consumer_key").asText();
                    Path credentials = child.resolve(oauthConsumerKey);
                    Files.writeString(credentials, obj.toString());

                    break;
                } catch (IOException ex) {
                    logger.warn("oauth_consumer_key not found.");
                }
            case "CodeCheckLTIResources": // primary key == resourceID
                try {
                    String resourceID = obj.get("resourceID").asText();
                    Path resource = child.resolve(resourceID);
                    Files.writeString(resource, obj.toString());

                    break;
                } catch (IOException ex) {
                    logger.warn("ResourceID not found.");
                }
            case "CodeCheckSubmissions": // primary key == submissionID, sortKey == submittedAt
                try {
                    String submissionID = obj.get("submissionID").asText();
                    Path submission = child.resolve(submissionID);
                    Files.createDirectory(submission);
                    String submittedAt = obj.get("submittedAt").toString();
                    Path submitted = submission.resolve(submissionID);
                    Files.writeString(submitted, obj.toString());

                    break;
                } catch (IOException ex) {
                    logger.warn("SubmissionID not found.");
                }
            case "CodeCheckWork": // primary key == assignmentID, sortkey == workID
                try {
                    String assignmentID = obj.get("assignmentID").asText();
                    Path assignment = child.resolve(assignmentID);
                    Files.createDirectory(assignment);
                    String workID = obj.get("workID").toString();
                    Path work = assignment.resolve(workID);
                    Files.writeString(work, obj.toString());

                    break;
                } catch (IOException ex) {
                    logger.warn("AssignmentID not found.");
                }
            default:
                logger.warn("Invalid Table Name.");
                break;
        }
    }

    public boolean writeNewerJsonObjectToDynamoDB(String tableName, ObjectNode obj, String primaryKeyName,
            String timeStampKeyName) {
        /*
         * To prevent a new item from replacing an existing item, use a conditional
         * expression that contains the attribute_not_exists function with the name of
         * the attribute being used as the partition key for the table. Since every
         * record must contain that attribute, the attribute_not_exists function will
         * only succeed if no matching item exists.
         * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SQLtoNoSQL.
         * WriteData.html
         * 
         * Apparently, the simpler putItem(item, conditionalExpression, nameMap,
         * valueMap) swallows the ConditionalCheckFailedException
         */
        String conditionalExpression = "attribute_not_exists(" + primaryKeyName + ") OR " + timeStampKeyName + " < :"
                + timeStampKeyName;
        if(amazonDynamoDB == null){
            //This is where the file logic function will be called

        } else{
            try {
                DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
                Table table = dynamoDB.getTable(tableName);
                table.putItem(
                        new PutItemSpec()
                                .withItem(Item.fromJSON(obj.toString()))
                                .withConditionExpression(conditionalExpression)
                                .withValueMap(Collections.singletonMap(":" + timeStampKeyName,
                                        obj.get(timeStampKeyName).asText())));
                return true;
            } catch (ConditionalCheckFailedException e) {
                // https://github.com/aws/aws-sdk-java/issues/1945
                logger.warn("writeNewerJsonObjectToDynamoDB: " + e.getMessage() + " " + obj);
                return false;
            }
        }
        return true;
    }

private boolean writeNewerJsonObjectToFile(String tableName, ObjectNode obj, String primaryKeyName, String timeStampKeyName) {

    String fileData;
    final String configVal = config.getString("com.horstmann.codecheck.db");
    Path base = Path.of(configVal);
    Path codeCheckWork = base.resolve("CodeCheckWork"); // /opt/codecheck/db/CodeCheckWork

    //Read existing file of the form CodeCheckWork/assignmentID/workID
    if(Files.isDirectory(codeCheckWork)){
        //Get sub-directories of CodeCheckWork directory (should only be the assignmentID sub-directory)
        File[] directories = new File(codeCheckWork.toString()).listFiles(File::isDirectory);
        for(File file : directories){
            try{
                Path workIDPath = Path.of(file.getAbsolutePath());
                fileData = Files.readString(workIDPath);
            } catch(IOException ex){
                logger.warn("WorkID file could not be read.");
            }
        }      
    } else{
        logger.warn("CodeCheckWork directory does not exist.");
        return false;
    }

    //Get timeStampKeyName val from read in file data
    String[] fileArray = fileData.split(",");
    String prevTimeStampVal;
    for(int i = 0; i < fileArray.length ; i++) {
        String[] items = fileArray[i].split(":");
        if(items[0].equals("timeStampKeyName")){//Key
            prevTimeStampVal = items[1]; // val
        }
    }
    String newTimeStampKeyVal = obj.get(timeStampKeyName).asText();

    //Compare both timeStampKeyVals
    if(newTimeStampKeyVal.compareTo(prevTimeStampVal) > 0){ // If new timeStampKeyVal is greater than the prev timeStampKeyVal, then we write new file
        Path child = base.resolve(tableName);
        Path path = child.resolve(primaryKeyName);
        writeFile(path, obj, newTimeStampKeyVal, prevTimeStampVal);
    }
    
    return true;
    }
    
    private void writeFile(Path path, ObjectNode obj, String newTimeStampKeyName, String prevTimeStampKeyName){
        boolean done = false;
        while (!done) {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        try {
            try (FileLock lock = channel.lock()) {
                ByteBuffer readBuffer = ByteBuffer.allocate((int)channel.size());
                channel.read(readBuffer);
                String jsonString = new String(readBuffer.array());
                // if jsonString is not empty, convert to JSON and read the timeStampKeyName
                if(!jsonString.isEmpty()){
                    
                } else if (jsonString.isEmpty() || newTimeStampKeyName.compareTo(prevTimeStampKeyName) < 0){// if jsonString is empty or that timeStampKeyName is older, write file like this
                    channel.truncate(0);
                    ByteBuffer writeBuffer = ByteBuffer.wrap(obj.toString().getBytes());
                    channel.write(writeBuffer);
                    done = true;
                }
       }
    } catch (Exception ex) {
       try {
          Thread.sleep(1000);
       } catch (InterruptedException ex2) {
       }
    }
    }
}
}