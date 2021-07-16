package com.cis555.search.crawler.remote;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.cis555.search.crawler.info.URLInfo;
import com.cis555.search.packet.*;

import static com.cis555.search.util.ObjectSerializer.*;

/**
 * Master node that is monitoring all the crawlers and dispatch crawl tasks.
 */
public class RemoteMaster {
	private DatagramSocket socket;
	private final ConcurrentHashMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
	
	private Thread sender;
	private Thread receiver;
	
	private static int MASTER_PORT = 21555;
	
	public RemoteMaster() {
		try {
			this.socket = new DatagramSocket(MASTER_PORT);
			this.socket.setSoTimeout(0);
		} catch (SocketException e) {
			e.printStackTrace();
		}
		this.sender = new Thread(this::sendFunc, "RemoteMaster-Sender");
		this.receiver = new Thread(this::receiveFunc, "RemoteControlMaster-Receiver");
		this.sender.start();
		this.receiver.start();
	}

	/**
	 * Handling the message based on the payload type.
	 */
	private final RemotePacket handle(RemotePacket inMessage, SocketAddress sender) {
		Object payload = inMessage.payload;
		if (payload instanceof HeartBeatPacket) {
			HeartBeatPacket packet = (HeartBeatPacket) payload;
			WorkerInfo workerInfo = workers.get(inMessage.source);
			if (workerInfo == null) {
				workerInfo = new WorkerInfo(inMessage.source, sender);
			}
			workerInfo.status = packet.status;
			workerInfo.lastTimestamp = System.nanoTime();
			workers.putIfAbsent(inMessage.source, workerInfo);
		} else if (payload instanceof StatPacket) {
			StatPacket packet = (StatPacket) payload;
			WorkerInfo workerInfo = workers.get(inMessage.source);
			if (workerInfo != null) {
				workerInfo.statistics = packet;
			}
			workerInfo.lastTimestamp = System.nanoTime();
		}
		
		return null;
	}
	
	/*
	 * Receiver Thread.
	 * */
	private void receiveFunc() {
		byte[] buf = new byte[65535];
		try {
			// cannot expect other nodes not to send after this node exits, so keep receiving until the end
			while (true) {
				try {
					// Receive Message
					DatagramPacket packet = new DatagramPacket(buf, buf.length);
					socket.receive(packet);
					
					// Extract Information
		            SocketAddress sender = packet.getSocketAddress();
		            RemotePacket inMessage = (RemotePacket) fromBinary(buf);
		            
		            // Message Handler
		            RemotePacket outMessage = this.handle(inMessage, sender);
		            if (outMessage == null) {
		            	continue;
		            }
		            
		            // Prepare new packet
		            byte[] outBytes = toBinary(outMessage);
		            DatagramPacket responsePacket = new DatagramPacket(outBytes, outBytes.length, sender);
		            
		            // Send Message
		            this.socket.send(responsePacket);
					
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	/*
	 * Sender Thread.
	 * Send out master broadcast packet periodically
	 * */
	private void sendFunc() {
		try {
			while (true) {
				try {
					ArrayList<String> workerNames = workers.keySet().stream().sorted().collect(Collectors.toCollection(ArrayList::new));
					ArrayList<String> workerAddrs =  workerNames.stream().map(name -> ((InetSocketAddress) workers.get(name).addr).getHostString()).collect(Collectors.toCollection(ArrayList::new));
					MasterBroadcastPacket payloadOut = new MasterBroadcastPacket(workerNames, workerAddrs);
					RemotePacket outMessage = new RemotePacket("master", payloadOut, String.valueOf(System.nanoTime()));
		            byte[] outBytes = toBinary(outMessage);
		            
		            for (WorkerInfo worker: workers.values()) {
		            	DatagramPacket responsePacket = new DatagramPacket(outBytes, outBytes.length, worker.addr);
						this.socket.send(responsePacket);
					}
					
				} catch (IOException e) {
					e.printStackTrace();
				}

				Thread.sleep(1000);
			}
			
		} catch (InterruptedException e) {

		}
	}

	/*
	 * Dispatch Urls to other nodes.
	 * */
	public String dispatch(String url) {
		// should be same impleementation as assignNodeId() in Dispatcher..
		ArrayList<String> workerNames = workers.keySet().stream().sorted().collect(Collectors.toCollection(ArrayList::new));
		int workerId = ((new URLInfo(url)).getHost().hashCode() & 0x7fffffff) % workers.size();
		String workerName = workerNames.get(workerId);
		WorkerInfo workerInfo = this.workers.get(workerName);
		
		// Get dest inet address
		if (workerInfo != null) {
			try {
				InetSocketAddress dest = (InetSocketAddress) workerInfo.addr;
		
				// Prepare payload
				String[] urls = new String[1];
				urls[0] = url;
				URLListPacket payloadOut = new URLListPacket(urls);
				RemotePacket outMessage = new RemotePacket("master",
						payloadOut, String.valueOf(System.nanoTime()));
				byte[] outBytes = toBinary(outMessage);
				DatagramPacket responsePacket = new DatagramPacket(outBytes, outBytes.length, dest);
			
				// Send packet
				this.socket.send(responsePacket);
				
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
			
			return workerName;
		} else {
			
			return null;
		}
		
	}
	
	public boolean requestStatistics(String workerName) {
		return this.sendControlPacket(workerName, ControlPacket.Reporting);
	}
	
	/*
	 * Send Control Packet to specific worker, based on their workerName.
	 * */
	private boolean sendControlPacket(String workerName, int messageType) {
		WorkerInfo workerInfo = this.workers.get(workerName);
		if (workerInfo == null) {
			return false;
		}
		try {
			ControlPacket payloadOut = new ControlPacket(messageType);
			RemotePacket outMessage = new RemotePacket("master", payloadOut, String.valueOf(System.nanoTime()));
			byte[] outBytes = toBinary(outMessage);
			
			DatagramPacket responsePacket = new DatagramPacket(outBytes, outBytes.length, workerInfo.addr);
			
			this.socket.send(responsePacket);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public final ArrayList<WorkerInfo> getWorkerInfoList() {
		ArrayList<String> workerNames = workers.keySet().stream().sorted().collect(Collectors.toCollection(ArrayList::new));
		return workerNames.stream().map(name -> workers.get(name)).collect(Collectors.toCollection(ArrayList::new));
	}
	
	public final WorkerInfo getWorkerInfoFromName(String workerName) {
		return workers.get(workerName);
	}
}