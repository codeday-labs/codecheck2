package models;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.horstmann.codecheck.Util;

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
import com.typesafe.config.ConfigException;

import play.Logger;

@Singleton
public class ProblemConnector {
    private ProblemConnection delegate;

    @Inject public ProblemConnector(Config config) {
        if (config.hasPath("com.horstmann.codecheck.s3.accessKey")) {
            delegate = new ProblemS3Connection(config);   
        }
        else {
            delegate = new ProblemLocalConnection(config);
        }
    }

    public void write(byte[] contents, String repo, String key) throws IOException {
        delegate.write(contents, repo, key);
    }

    public void delete(String repo, String key) throws IOException {
        delegate.delete(repo, key);
    }

    public byte[] read(String repo, String key) throws IOException {
        return delegate.read(repo, key);
    }
}

interface ProblemConnection {
    public void write(byte[] contents, String repo, String key) throws IOException;
    public void delete(String repo, String key) throws IOException;
    public byte[] read(String repo, String key) throws IOException;
}

class ProblemS3Connection implements ProblemConnection {
    private Config config;
    private String bucketSuffix = null;
    private AmazonS3 amazonS3;
    private static Logger.ALogger logger = Logger.of("com.horstmann.codecheck");

    public ProblemS3Connection(Config config) {
        this.config = config;
        String s3AccessKey = config.getString("com.horstmann.codecheck.s3.accessKey");
        String s3SecretKey = config.getString("com.horstmann.codecheck.s3.secretKey");
        String s3Region = config.getString("com.horstmann.codecheck.s3.region"); 
        amazonS3 = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(s3AccessKey, s3SecretKey)))
                .withRegion(s3Region)
                .withForceGlobalBucketAccessEnabled(true)
                .build();

        bucketSuffix = config.getString("com.horstmann.codecheck.s3bucketsuffix");
    }

    public boolean isOnS3(String repo) {
        String key = "com.horstmann.codecheck.repo." + repo;
        return !config.hasPath(key) || config.getString(key).isEmpty();
    }

    public boolean isOnS3(String repo, String key) {
        String bucket = repo + "." + bucketSuffix;
        return amazonS3.doesObjectExist(bucket, key);            
    }
    
    public void write(Path file, String repo, String key) throws IOException {
        String bucket = repo + "." + bucketSuffix;
        try {
            amazonS3.putObject(bucket, key, file.toFile());
        } catch (AmazonS3Exception ex) {
            logger.error("S3Connection.putToS3: Cannot put " + file + " to " + bucket);
            throw ex;
        }
    }

    public void write(String contents, String repo, String key) throws IOException {
        String bucket = repo + "." + bucketSuffix;
        try {
            amazonS3.putObject(bucket, key, contents);
        } catch (AmazonS3Exception ex) {
            logger.error("S3Connection.putToS3: Cannot put " + contents.replaceAll("\n", "|").substring(0, Math.min(50, contents.length())) + "... to " + bucket);
            throw ex;
        }
    }

    public void write(byte[] contents, String repo, String key) throws IOException {
        String bucket = repo + "." + bucketSuffix;
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contents.length);
        metadata.setContentType("application/zip");
        try {
            try (ByteArrayInputStream in = new ByteArrayInputStream(contents)) {
                amazonS3.putObject(bucket, key, in, metadata); 
            }                 
        } catch (AmazonS3Exception ex) {
            String bytes = Arrays.toString(contents);
            logger.error("S3Connection.putToS3: Cannot put " + bytes.substring(0, Math.min(50, bytes.length())) + "... to " + bucket);
            throw ex;                
        }
    }

    public void delete(String repo, String key) throws IOException {
        String bucket = repo + "." + bucketSuffix;
        try {
            amazonS3.deleteObject(bucket, key);
        } catch (AmazonS3Exception ex) {
            logger.error("S3Connection.deleteFromS3: Cannot delete " + bucket);
            throw ex;
        }            
    }

    public byte[] read(String repo, String key) throws IOException {
        String bucket = repo + "." + bucketSuffix;

        byte[] bytes = null;
        try {
            // TODO -- trying to avoid warning 
            // WARN - com.amazonaws.services.s3.internal.S3AbortableInputStream - Not all bytes were read from the S3ObjectInputStream, aborting HTTP connection. This is likely an error and may result in sub-optimal behavior. Request only the bytes you need via a ranged GET or drain the input stream after use
            try (InputStream in = amazonS3.getObject(bucket, key).getObjectContent()) {
                bytes = in.readAllBytes();
            }
        } catch (AmazonS3Exception ex) {
            logger.error("S3Connection.readFromS3: Cannot read " + key + " from " + bucket);
            throw ex;
        }
        return bytes;            
    }
}

class ProblemLocalConnection implements ProblemConnection {
    private Config config;
    private static Logger.ALogger logger = Logger.of("com.horstmann.codecheck");

    public ProblemLocalConnection(Config config) {
        this.config = config;
    }

    public void write(byte[] contents, String repo, String key) throws IOException {
        try {
            Path repoPath = Path.of(config.getString("com.horstmann.codecheck.repo." + repo));
            Path problemDir = repoPath.resolve(key);
            Util.deleteDirectory(problemDir); // Delete any prior contents so that it is replaced by new zip file
            Files.createDirectories(problemDir);                
            Path newFilePath = problemDir.resolve(key+".zip");
            org.apache.commons.io.FileUtils.writeByteArrayToFile(new File(newFilePath.toString()), contents);
        } catch (IOException ex) {
            String bytes = Arrays.toString(contents);
            logger.error("ProblemLocalConnection.write : Cannot put " + bytes.substring(0, Math.min(50, bytes.length())) + "... to " + repo);
            throw ex;                   
        }

    }

    public void delete(String repo, String key) throws IOException {
        String repoPath = config.getString("com.horstmann.codecheck.repo." + repo);
        Path directoryPath = Path.of(repoPath).resolve(key);
        try {
            Util.deleteDirectory(directoryPath);
        } catch (IOException ex) {
            logger.error("ProblemLocalConnection.delete : Cannot delete " + repo);
            throw ex;
        }
    }
    
    public byte[] read(String repo, String key) throws IOException {
        byte[] result = null;
        try {
            Path repoPath = Path.of(config.getString("com.horstmann.codecheck.repo." + repo));
            if (key.startsWith("/"))
                key = key.substring(1);
                            
            Path filePath = repoPath.resolve(key).resolve(key+".zip");
            result = Files.readAllBytes(filePath); 
        } catch (IOException ex) {
            logger.error("ProblemLocalConnection.read : Cannot read " + key + " from " + repo);
            throw ex;                
        }
        
        return result;  
    }
}

