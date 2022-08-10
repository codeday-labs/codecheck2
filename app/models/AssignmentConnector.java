package models;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileWriter;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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

import play.Logger;

@Singleton class AssignmentConnector {
    private AssignmentConnection delegate;

    @Inject public AssignmentConnector(Config config) {
        if (config.hasPath("com.horstmann.codecheck.s3.accessKey")) {
            delegate = new AssignmentS3Connection(config);      
        }
        else {
            delegate = new AssignmentLocalConnection(config);    
        }    
    }

    public ObjectNode readJsonObjectFromDB(String tableName, String primaryKeyName, String primaryKeyValue) throws IOException {
        return delegate.readJsonObjectFromDB(tableName, primaryKeyName, primaryKeyValue);
    }
    public String readJsonStringFromDB(String tableName, String primaryKeyName, String primaryKeyValue) throws IOException {
        return delegate.readJsonStringFromDB(tableName, primaryKeyName, primaryKeyValue);
    }
    public ObjectNode readJsonObjectFromDB(String tableName, String primaryKeyName, String primaryKeyValue, String sortKeyName, String sortKeyValue) throws IOException {
        return delegate.readJsonObjectFromDB(tableName, primaryKeyName, primaryKeyValue, sortKeyName, sortKeyValue);
    }
    public ObjectNode readNewestJsonObjectFromDB(String tableName, String primaryKeyName, String primaryKeyValue) {
        return delegate.readNewestJsonObjectFromDB(tableName, primaryKeyName, primaryKeyValue);
    }
    public String readJsonStringFromDB(String tableName, String primaryKeyName, String primaryKeyValue, String sortKeyName, String sortKeyValue) throws IOException {
        return delegate.readJsonStringFromDB(tableName, primaryKeyName, primaryKeyValue, sortKeyName, sortKeyValue);
    }
    public Map<String, ObjectNode> readJsonObjectsFromDB(String tableName, String primaryKeyName, String primaryKeyValue, String sortKeyName) throws IOException, ParseException {
        return delegate.readJsonObjectsFromDB(tableName, primaryKeyName, primaryKeyValue, sortKeyName);
    }
    public void writeJsonObjectToDB(String tableName, ObjectNode obj) {
        delegate.writeJsonObjectToDB(tableName, obj);
    }
    public boolean writeNewerJsonObjectToDB(String tableName, ObjectNode obj, String primaryKeyName, String timeStampKeyName) {
        return delegate.writeNewerJsonObjectToDB(tableName, obj, primaryKeyName, timeStampKeyName);
    }        
}

interface AssignmentConnection {
    default ObjectNode readJsonObjectFromDB(String tableName, String primaryKeyName, String primaryKeyValue) throws IOException {
        String result = readJsonStringFromDB(tableName, primaryKeyName, primaryKeyValue);
        return result == null ? null : (ObjectNode)(new ObjectMapper().readTree(result)); 
    }
    
    default ObjectNode readJsonObjectFromDB(String tableName, String primaryKeyName, String primaryKeyValue, String sortKeyName, String sortKeyValue) throws IOException {
        String result = readJsonStringFromDB(tableName, primaryKeyName, primaryKeyValue, sortKeyName, sortKeyValue);
        return result == null ? null : (ObjectNode)(new ObjectMapper().readTree(result)); 
    }
    
    public String readJsonStringFromDB(String tableName, String primaryKeyName, String primaryKeyValue) throws IOException;
    public ObjectNode readNewestJsonObjectFromDB(String tableName, String primaryKeyName, String primaryKeyValue);
    public String readJsonStringFromDB(String tableName, String primaryKeyName, String primaryKeyValue, String sortKeyName, String sortKeyValue) throws IOException;
    public Map<String, ObjectNode> readJsonObjectsFromDB(String tableName, String primaryKeyName, String primaryKeyValue, String sortKeyName) throws IOException, ParseException;
    public void writeJsonObjectToDB(String tableName, ObjectNode obj);
    public boolean writeNewerJsonObjectToDB(String tableName, ObjectNode obj, String primaryKeyName, String timeStampKeyName);

}

class AssignmentS3Connection implements AssignmentConnection {
    private Config config;
    private String bucketSuffix = null;
    private AmazonDynamoDB amazonDynamoDB;
    private static Logger.ALogger logger = Logger.of("com.horstmann.codecheck");
    public static class OutOfOrderException extends RuntimeException {}

    public AssignmentS3Connection(Config config) {
        this.config = config;
        String s3AccessKey = config.getString("com.horstmann.codecheck.s3.accessKey");
        String s3SecretKey = config.getString("com.horstmann.codecheck.s3.secretKey");
        String s3Region = config.getString("com.horstmann.codecheck.s3.region"); 
        
        amazonDynamoDB = AmazonDynamoDBClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(s3AccessKey, s3SecretKey)))
                .withRegion("us-west-1")
                .build();
            
        bucketSuffix = config.getString("com.horstmann.codecheck.s3bucketsuffix");            
    }    
    
    public String readJsonStringFromDB(String tableName, String primaryKeyName, String primaryKeyValue) throws IOException {
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
    
    public ObjectNode readNewestJsonObjectFromDB(String tableName, String primaryKeyName, String primaryKeyValue) {
        DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
        Table table = dynamoDB.getTable(tableName); 
        QuerySpec spec = new QuerySpec()
                .withKeyConditionExpression(primaryKeyName + " = :primaryKey" )
                .withValueMap(new ValueMap().withString(":primaryKey", primaryKeyValue))
                .withScanIndexForward(false);
        
        ItemCollection<QueryOutcome> items = table.query(spec);
        try {
            Iterator<Item> iterator = items.iterator();
            if (iterator.hasNext()) {
                String result = iterator.next().toJSON();
                try {
                    return (ObjectNode)(new ObjectMapper().readTree(result));
                } catch (JsonProcessingException ex) {
                    return null;
                }
            }
            else
                return null;
        } catch (ResourceNotFoundException ex) {
            return null;
        }
    }        

    public String readJsonStringFromDB(String tableName, String primaryKeyName, String primaryKeyValue, String sortKeyName, String sortKeyValue) throws IOException {
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

    public Map<String, ObjectNode> readJsonObjectsFromDB(String tableName, String primaryKeyName, String primaryKeyValue, String sortKeyName) throws IOException {
        DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
        Table table = dynamoDB.getTable(tableName); 
        ItemCollection<QueryOutcome> items = table.query(primaryKeyName, primaryKeyValue);
        Iterator<Item> iterator = items.iterator();
        Map<String, ObjectNode> itemMap = new HashMap<>();
        while (iterator.hasNext()) {
            Item item = iterator.next();
            String key = item.getString(sortKeyName);
            itemMap.put(key, (ObjectNode)(new ObjectMapper().readTree(item.toJSON())));
        }
        return itemMap;
    }

    public void writeJsonObjectToDB(String tableName, ObjectNode obj) {
        DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
        Table table = dynamoDB.getTable(tableName); 
        table.putItem(
            new PutItemSpec()
                .withItem(Item.fromJSON(obj.toString()))
        );
    }

    public boolean writeNewerJsonObjectToDB(String tableName, ObjectNode obj, String primaryKeyName, String timeStampKeyName) {
        DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
        Table table = dynamoDB.getTable(tableName);
            /*
    To prevent a new item from replacing an existing item, use a conditional expression that contains the attribute_not_exists function with the name of the attribute being used as the partition key for the table. Since every record must contain that attribute, the attribute_not_exists function will only succeed if no matching item exists.
    https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SQLtoNoSQL.WriteData.html

    Apparently, the simpler putItem(item, conditionalExpression, nameMap, valueMap) swallows the ConditionalCheckFailedException 
             */
        String conditionalExpression = "attribute_not_exists(" + primaryKeyName + ") OR " + timeStampKeyName + " < :" + timeStampKeyName;
        try {
            table.putItem(
                new PutItemSpec()
                    .withItem(Item.fromJSON(obj.toString()))
                    .withConditionExpression(conditionalExpression)
                    .withValueMap(Collections.singletonMap(":" + timeStampKeyName, obj.get(timeStampKeyName).asText())));
            return true;
        } catch(ConditionalCheckFailedException e) {
            // https://github.com/aws/aws-sdk-java/issues/1945
            logger.warn("writeNewerJsonObjectToDynamoDB: " + e.getMessage() + " " + obj);
            return false;
        }   
    }
}

class AssignmentLocalConnection implements AssignmentConnection {

    private Config config;
    private static Logger.ALogger logger = Logger.of("com.horstmann.codecheck");
    public AssignmentLocalConnection(Config config){
        this.config = config;
    }

    public String readJsonStringFromDB(String tableName, String primaryKeyName, String primaryKeyValue) throws IOException {
        Path repoPath = Path.of(config.getString("com.horstmann.codecheck.db") + "/" + tableName);
        Path jsonFile = repoPath.resolve(primaryKeyValue);

        try {
            String result = Files.readString(jsonFile);
            return result;
        } catch (IOException ex) {
            return null;
        }
    }

    public ObjectNode readNewestJsonObjectFromDB(String tableName, String primaryKeyName, String primaryKeyValue) {
        return null;
    }
    
    public String readJsonStringFromDB(String tableName, String primaryKeyName, String primaryKeyValue, String sortKeyName, String sortKeyValue) throws IOException {
        Path repoPath = Path.of(config.getString("com.horstmann.codecheck.db") + "/" + tableName + "/" + primaryKeyValue);
        Path jsonFile = repoPath.resolve(sortKeyValue);

        try {
            String result = Files.readString(jsonFile);
            return result;
        } catch (IOException ex) {
            return null;
        }

    }
    public Map<String, ObjectNode> readJsonObjectsFromDB(String tableName, String primaryKeyName, String primaryKeyValue, String sortKeyName) throws IOException, ParseException {
        Map<String, ObjectNode> itemMap = new HashMap<>();
        Path pathToDirectory = Path.of(config.getString("com.horstmann.codecheck.db") + "/" + tableName + "/" + primaryKeyValue);

        try (Stream<Path> entries = Files.list(pathToDirectory)) {
            List<Path> files = entries.filter(Files::isRegularFile).collect(Collectors.toList());
            for (Path file : files) {
                String fileData = Files.readString(file);
                JSONParser parser = new JSONParser();
                JSONObject json = (JSONObject) parser.parse(fileData);
                String key = (String) json.get(sortKeyName);
                itemMap.put(key, (ObjectNode) (new ObjectMapper().readTree(json.toJSONString())));
            }
        }
        return itemMap;
    }
    public void writeJsonObjectToDB(String tableName, ObjectNode obj) {
        
        // Create a directory for the given table
        // E.g. if the key "com.horstmann.codecheck.db" has the value
        // "/opt/codecheck/db", then the directory created should be
        final String configVal = config.getString("com.horstmann.codecheck.db");

        Path base = Path.of(configVal);
        Path child = base.resolve(tableName);
        try {
            Files.createDirectories(child); // Should create a directory with the path
                                            // /opt/codecheck/db/CodeCheckAssignments
        } catch (IOException ex) {
            logger.warn(tableName + " directory could not be generated");
        }
        try {
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
                        logger.warn(ex.getMessage());
                    }
                case "CodeCheckLTICredentials": // primary key == oauth_consumer_key
                    try {
                        String oauthConsumerKey = obj.get("oauth_consumer_key").asText();
                        Path credentials = child.resolve(oauthConsumerKey);
                        Files.writeString(credentials, obj.toString());

                        break;
                    } catch (IOException ex) {
                        logger.warn(ex.getMessage());
                    }
                case "CodeCheckLTIResources": // primary key == resourceID
                    try {
                        String resourceID = obj.get("resourceID").asText();
                        Path resource = child.resolve(resourceID);
                        Files.writeString(resource, obj.toString());
                        break;
                    } catch (IOException ex) {
                        logger.warn(ex.getMessage());
                    }
                case "CodeCheckSubmissions": // primary key == submissionID, sortKey == submittedAt
                    try {
                        String submissionID = obj.get("submissionID").asText();
                        Path submission = child.resolve(submissionID);
                        Files.createDirectory(submission);
                        String submittedAt = obj.get("submittedAt").asText();
                        Path submitted = submission.resolve(submissionID);
                        Files.writeString(submitted, obj.toString());
                        break;
                    } catch (IOException ex) {
                        logger.warn(ex.getMessage());
                    }
                case "CodeCheckWork": // primary key == assignmentID, sortkey == workID
                    try {
                        String assignmentID = obj.get("assignmentID").asText();
                        String workID = obj.get("workID").asText();
                        Path assignment = child.resolve(assignmentID);
                        Files.createDirectory(assignment);
                        Path work = assignment.resolve(workID);
                        Files.writeString(work, obj.toString());
                        break;
                    } catch (IOException ex) {
                        logger.warn(ex.getMessage());
                    }
                default:
                    logger.warn("Invalid Table Name.");
                    break;
            }
        } catch (NullPointerException ex) {
            logger.warn("Switch Statement error.");
        }
    }
    public boolean writeNewerJsonObjectToDB(String tableName, ObjectNode obj, String primaryKeyName, String timeStampKeyName) {
        
        String fileData = new String();
        final String configVal = config.getString("com.horstmann.codecheck.db");
        Path base = Path.of(configVal);
        Path codeCheckWork = base.resolve("CodeCheckWork"); // /opt/codecheck/db/CodeCheckWork

        // Read existing file of the form CodeCheckWork/assignmentID/workID
        if (Files.isDirectory(codeCheckWork)) {
            // Get sub-directories of CodeCheckWork directory (should only be the
            // assignmentID sub-directory)
            try(Stream <Path> entries = Files.list(codeCheckWork)){
                List<Path> directories = entries.filter(Files::isDirectory).collect(Collectors.toList());
                for (Path file: directories) {
                    try {
                        fileData = Files.readString(file);
                    } catch (IOException ex) {
                        logger.warn("WorkID file could not be read.");
                    }
                }
            } catch(IOException ex){
                logger.warn(ex.getMessage());
            }
        }else {
            logger.warn("CodeCheckWork directory does not exist.");
            return false;
        }

        // Get timeStampKeyName val from read in file data
        String[] fileArray = fileData.split(",");
        String prevTimeStampVal = new String();
        for (int i = 0; i < fileArray.length; i++) {
            String[] items = fileArray[i].split(":");
            if (items[0].equals("timeStampKeyName")) { // Key
                prevTimeStampVal = items[1]; // val
            }
        }
        // String newTimeStampKeyVal = obj.get(timeStampKeyName).asText();

        // Compare both timeStampKeyVals
        try {
            Path child = base.resolve(tableName);
            Path path = child.resolve(primaryKeyName);
            writeFileToNewestTimeStamp(path, obj, timeStampKeyName, prevTimeStampVal);
        } catch (Exception ex) {
            logger.warn("There was an error writing the newer JSON object to a file.");
            return false;
        }

        return true;
    }

    private void writeFileToNewestTimeStamp(Path path, ObjectNode obj, String newTimeStampKeyName,
            String prevTimeStampVal) throws ParseException{
        String newTimeStampVal = new String();
        boolean done = false;
        while (!done) {
            try {
                FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE);
                try {
                    try (FileLock lock = channel.lock()) {
                        ByteBuffer readBuffer = ByteBuffer.allocate((int) channel.size());
                        channel.read(readBuffer);
                        String jsonString = new String(readBuffer.array());
                        // if jsonString is not empty, convert to JSON and read the timeStampKeyName
                        if (!jsonString.isEmpty()) {
                            JSONParser parser = new JSONParser();
                            JSONObject json = (JSONObject) parser.parse(jsonString);
                            newTimeStampVal = (String) json.get(newTimeStampKeyName);
                        }
                        // if jsonString is empty or that timeStampKeyName is older, write file like this
                        if (jsonString.isEmpty() || newTimeStampVal.compareTo(prevTimeStampVal) < 0) {
                            channel.truncate(0);
                            ByteBuffer writeBuffer = ByteBuffer.wrap(obj.toString().getBytes());
                            channel.write(writeBuffer);
                        }
                        done = true;
                    }
                } catch (Exception ex) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex2) {
                    }
                }
            } catch (IOException ex) {
                logger.warn("There was an error in the writeFileToNewestTimeStamp function. Error message: "
                        + ex.getMessage());
            }
        }
    }
}