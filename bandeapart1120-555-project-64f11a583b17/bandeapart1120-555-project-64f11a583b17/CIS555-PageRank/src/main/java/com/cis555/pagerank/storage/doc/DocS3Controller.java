package com.cis555.pagerank.storage.doc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/*
 * Controller to communicate with S3, which stores all real data about the documents.
 * */
public class DocS3Controller implements AutoCloseable {

    // AWS S3
    private static final String BUCKET_NAME = "searchengine-cis555-dev";
    private static Region region = Region.US_EAST_1;
    private static AwsCredentialsProvider creds = () -> AwsBasicCredentials
            .create("AKIA47OF6EWHIAKS6MFQ", "2M+VCmKlK2Ei/3XlIksu1iaTqT6me+W4vR+pNi36");
    private static final S3Client s3 = S3Client.builder().region(region).credentialsProvider(creds).build();

    // Internal Data Structuregi
    private DocS3Block currentBlock = null;
    private String nodeIdentifier = null;
    private final String storageDir = "./temp/";

    // Parameters
    static private final int BLOCK_SIZE = 1 * 1024 * 1024;

    private static Logger logger = LoggerFactory.getLogger(DocS3Controller.class);

    // Use this constructor if you want to read document contents only
    public DocS3Controller() {
        (new File(storageDir)).mkdirs();
    }

    // Use this constructor if you want to create new document
    public DocS3Controller(String nodeIdentifier) {
        if (nodeIdentifier.length() >= 10) {
            throw new RuntimeException("Node Identifier should be shorter than 10 bytes!");
        }
        (new File(storageDir)).mkdirs();
        this.nodeIdentifier = nodeIdentifier;
        this.newBlock();
    }

    /*
     * Create new block.
     * */
    private synchronized void newBlock() {
        long nowTs = System.currentTimeMillis() / 1000;
        this.currentBlock = new DocS3Block(nodeIdentifier + "-" + nowTs);
        logger.info("New Block Created: " + this.currentBlock.getBlockName() + " .");
    }

    /*
     * Create new block if the current block is full.
     * */
    private synchronized void check(int contentbytes) {
        // Block Size = 200 MB by default
        if (currentBlock.getTotalBytes() + DocS3Block.getHeaderLength() + contentbytes > BLOCK_SIZE) {

            // Save old block to file and then upload...
            final DocS3Block blockToSave = currentBlock;
            Thread t = new Thread(() -> {
                blockToSave.saveToFile(storageDir);
                logger.info("Current Block " + blockToSave.getBlockName() + " has " + blockToSave.getEntityCount() + " entities, which is saved automatically.");
                uploadFileToS3(Paths.get(storageDir, blockToSave.getBlockName()), blockToSave.getBlockName());
                logger.info("Current Block " + blockToSave.getBlockName() + " has been uploaded to S3.");
                Paths.get(storageDir, blockToSave.getBlockName()).toFile().delete();
                blockToSave.reset(); // may help gc?
            });
            t.start();

            // Create new block
            this.newBlock();
        }
    }

    /*
     * Add document to current block.
     * */
    public final List<Object> addDoc(String url, short contentType, byte[] contentBytes, byte[] docId, byte[] urlId) {
        this.check(contentBytes.length);
        String blockName = this.currentBlock.getBlockName();
        int blockIndex = this.currentBlock.addDoc(url, contentType, contentBytes, docId, urlId);
        return Arrays.asList(blockIndex, blockName); // blockIndex, blockName
    }

    public DocS3Entity querySingleDoc(String blockName, int index) {
        return querySingleDoc(blockName, index, false);
    }

    /*
     * Query a document.
     * Perform HTTP partial download.
     * */
    public DocS3Entity querySingleDoc(String blockName, int index, boolean isCompressed) {
        byte[] headerByte = downloadFileRangeFromS3(blockName, 4 + DocS3Block.getHeaderLength() * index, DocS3Block.getHeaderLength());
        DocS3Entity docS3Entity = new DocS3Entity();
        int offset = DocS3Block.loadHeader(headerByte, docS3Entity);
        byte[] resultByte = downloadFileRangeFromS3(blockName, offset, docS3Entity.getContentLength());
        docS3Entity.setContentBytes(resultByte);
        return docS3Entity;
    }

    /*
     * Download the entire block from S3 and return an instance of content block.
     * */
    public DocS3Block getEntireDocBlock(String blockName) {
        Path path = Paths.get(storageDir, blockName);
        path.toFile().delete();
        downloadFileFromS3(path, blockName);
        DocS3Block docS3Block = new DocS3Block(blockName);
        docS3Block.loadFromFile(storageDir);
        path.toFile().delete();
        return docS3Block;
    }

    private static void uploadFileToS3(Path path, String blockName) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(blockName)
                .build();
        @SuppressWarnings("unused")
        PutObjectResponse putObjectResponse = s3.putObject(putObjectRequest, path);
    }

    private static byte[] downloadFileRangeFromS3(String blockName, int beginPos, int length) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(blockName)
                .range("bytes=" + beginPos + "-" + (beginPos + length - 1)) // inclusive
                .build();
        ResponseBytes<GetObjectResponse> getObjectResponseBytes = s3.getObjectAsBytes(getObjectRequest);
        return getObjectResponseBytes.asByteArray();
    }


    // path is supposed to include the filename here.
    // If you're going to use the blockName as the filename, let path=Paths.get("/the/storage/directory/", blockName);
    public static void downloadFileFromS3(Path path, String blockName) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(blockName)
                .build();
        @SuppressWarnings("unused")
        GetObjectResponse getObjectResponse = s3.getObject(getObjectRequest, path);
    }

    public static List<String> listFilesInS3() {
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
        // https://docs.aws.amazon.com/AmazonS3/latest/dev/ListingObjectKeysUsingJava.html
        List<String> result = new ArrayList<String>();

        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket(BUCKET_NAME)
                .build();
        ListObjectsV2Response listObjectsV2Response;

        do {
            // Issue the request
            listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);

            // Collect results
            List<S3Object> s3Objects = listObjectsV2Response.contents();
            result.addAll(s3Objects.stream().map(S3Object::key).collect(Collectors.toList()));

            // Check whether more objects
            String token = listObjectsV2Response.nextContinuationToken();
            listObjectsV2Request = listObjectsV2Request.toBuilder().continuationToken(token).build();

        } while (listObjectsV2Response.isTruncated());

        return result;
    }

    @Override
    public synchronized void close() {
        if (this.currentBlock != null && this.currentBlock.getEntityCount() > 0) {
            final DocS3Block blockToSave = currentBlock;
            blockToSave.saveToFile(storageDir);
            logger.info("Current Block " + this.currentBlock.getBlockName() + " has " + this.currentBlock.getEntityCount() + " entities, which is saved automatically.");
            uploadFileToS3(Paths.get(storageDir, blockToSave.getBlockName()), blockToSave.getBlockName());
            logger.info("Current Block " + this.currentBlock.getBlockName() + " has been uploaded to S3.");
            Paths.get(storageDir, blockToSave.getBlockName()).toFile().delete();
            blockToSave.reset();
        } else {
            logger.info("Current Block " + this.currentBlock.getBlockName() + " has " + this.currentBlock.getEntityCount() + " entity to save.");
        }
    }

    ;

    public static void main(String[] args) {
        List<String> files = listFilesInS3();
        DocS3Controller docS3Controller = new DocS3Controller();
        DocS3Entity docS3Entity = docS3Controller.querySingleDoc("Test-T02-1620126500", 1, false);
        String docId = new String(docS3Entity.getDocId());
        String urlId = new String(docS3Entity.getUrlId());
        String content = new String(docS3Entity.getContentBytes());
        System.out.println(docId);
        System.out.println(urlId);
        System.out.println(content);
        System.out.println("Hello");
    }

}
