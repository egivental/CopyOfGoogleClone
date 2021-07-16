package com.cis555.search.storage;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import com.cis555.search.enums.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/**
 * Controller to communicate with S3.
 * S3 is storing all the raw bytes of the documents and its header information
 */
public class DocS3Contoller implements AutoCloseable {
	private static Logger logger = LoggerFactory.getLogger(DocS3Contoller.class);
	private static int BLOCK_SIZE = Integer.parseInt(Constants.BLOCK_SIZE.value());

	//S3 credential. AWS educate account
	private static final String BUCKET_NAME = "searchengine-cis555-qa";
	private static Region region = Region.US_EAST_1;
	private static AwsCredentialsProvider creds = () ->
			AwsBasicCredentials.create("AKIA47OF6EWHIAKS6MFQ", "2M+VCmKlK2Ei/3XlIksu1iaTqT6me+W4vR+pNi36");
	private static final S3Client s3 = S3Client.builder().region(region).credentialsProvider(creds).build();

	/**
	 * All the S3Block will be saved as a local file before uploaded to cloud
	 */
	private DocS3Block currBlock = null;
	private String nodeId = null;
	private final String storageDir = "./temp/";

	public DocS3Contoller(String nodeId) {
		if (nodeId.length() >= 10) {
			throw new RuntimeException("Node Identifier too long");
		}
		new File(storageDir).mkdirs();
		this.nodeId = nodeId;
		this.newBlock();
	}

	/**
	 * Create a new block. The block name is the node-timestamp
	 */
	private synchronized void newBlock() {
		this.currBlock = new DocS3Block(nodeId + "-" + System.currentTimeMillis()/1000);
		logger.info("New Block Created: {}", this.currBlock.getBlockName());
	}

	/**
	 * Check if the old block is full or not.
	 * If full, upload it to S3 and remove the old block.
	 */
	private synchronized void check(int contentSize) {
		if (currBlock.getTotalBytes() + DocS3Block.getHeaderLength() + contentSize > BLOCK_SIZE) {
			final DocS3Block blockToSave = currBlock;
			new Thread(()->{
				blockToSave.saveToFile(storageDir);
				logger.info("Current Block {} with {} documents is saved.", blockToSave.getBlockName(), blockToSave.getEntityCount());
				uploadFileToS3(Paths.get(storageDir, blockToSave.getBlockName()), blockToSave.getBlockName());
				logger.info("Current Block {} has been uploaded to S3.", blockToSave.getBlockName());
				Paths.get(storageDir, blockToSave.getBlockName()).toFile().delete();
				blockToSave.reset();
			}).start();
			this.newBlock();
		}
	}

	/**
	 * Adding new document to current block.
	 * Parsing a list of object for reference
	 */
	public final List<Object> addDoc(String url, short contentType, byte[] contentBytes, byte[] docId, byte[] urlId) {
		this.check(contentBytes.length);
		String blockName = this.currBlock.getBlockName();
		int blockIndex = this.currBlock.addDoc(url, contentType, contentBytes, docId, urlId);
		return Arrays.asList(blockIndex, blockName);
	}

	private static void uploadFileToS3(Path path, String blockName) {
		PutObjectRequest putObjectRequest = PutObjectRequest.builder()
				.bucket(BUCKET_NAME)
				.key(blockName)
				.build();
		PutObjectResponse putObjectResponse = s3.putObject(putObjectRequest, path);
		logger.info("Successfully up load to S3. Response: {}", putObjectResponse.eTag());
	}
	
	@Override
	public synchronized void close() {
		if (this.currBlock != null && this.currBlock.getEntityCount() > 0) {
			final DocS3Block blockToSave = currBlock;
			blockToSave.saveToFile(storageDir);
			logger.info("Current Block " + this.currBlock.getBlockName() + " has " + this.currBlock.getEntityCount() + " entities, which is saved automatically.");
			uploadFileToS3(Paths.get(storageDir, blockToSave.getBlockName()), blockToSave.getBlockName());
			logger.info("Current Block " + this.currBlock.getBlockName() + " has been uploaded to S3.");
			Paths.get(storageDir, blockToSave.getBlockName()).toFile().delete();
			blockToSave.reset();
		} else {
			logger.info("Current Block " + this.currBlock.getBlockName() + " has " + this.currBlock.getEntityCount() + " entity to save.");
		}
	};
}
