package com.cis555.search.crawler.remote;

import com.cis555.search.crawler.Crawler;
import com.cis555.search.crawler.TaskDispatcher;
import com.cis555.search.packet.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.cis555.search.util.ObjectSerializer.fromBinary;
import static com.cis555.search.util.ObjectSerializer.toBinary;

public class RemoteWorker {
    private static Logger logger = LoggerFactory.getLogger(RemoteWorker.class);
    private static int PORT = 20556;
    private DatagramSocket socket;
    private final InetSocketAddress masterSocketAddr;
    private final Crawler crawler;
    private TaskDispatcher dispatcher;
    private List<String> names;
    private List<InetSocketAddress> addrs;
    private boolean isRunning = true;
    private Thread sender;
    private Thread receiver;

    public RemoteWorker(InetSocketAddress masterSocketAddr, Crawler crawler) {
        this.names = new ArrayList<>();
        this.addrs = new ArrayList<>();
        this.crawler = crawler;
        this.masterSocketAddr = masterSocketAddr;
        try {
            if (this.masterSocketAddr != null) {
                this.socket = new DatagramSocket(PORT);
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    public void initalize() {
        if (this.masterSocketAddr == null) {
            return;
        }
        this.dispatcher = crawler.getDispatcher();
        this.sender = new Thread(this::send, "RemoteSender");
        this.receiver = new Thread(this::receive, "RemoteReceiver");
        this.sender.start();
        this.receiver.start();
    }

    public final RemotePacket handle(RemotePacket inMessage) {
        Serializable payloadOut = null;
        Object payload = inMessage.payload;
        if (payload instanceof ControlPacket) {
            ControlPacket packet = (ControlPacket) payload;
            if (packet.signal == ControlPacket.Reporting) {
                payloadOut = new StatPacket(crawler.getHTMLCounts());
            }
        } else if (payload instanceof URLListPacket) {
            URLListPacket packet = (URLListPacket) payload;
            String[] urls = packet.urls;
            for (String url : urls) {
                dispatcher.getTaskFromRemote(url);
            }
            String source = inMessage.source;
            if (source.equals("master")) {
                logger.info("Master URL Dispatching: {}", packet.urls.toString());
            }
        } else if (payload instanceof MasterBroadcastPacket) {
            MasterBroadcastPacket packet = (MasterBroadcastPacket) payload;
            this.names.clear();
            this.names.addAll(packet.getNames());
            this.addrs = packet.getAddrs().stream().map(ip -> new InetSocketAddress(ip, PORT)).collect(Collectors.toList());
        }
        return payloadOut == null ? null : new RemotePacket(crawler.getCrawlerIdentifier(), payloadOut, String.valueOf(System.nanoTime()));
    }

    private void receive() {
        byte[] buf = new byte[65535];
        while (isRunning) {
            try {
                // 1. Receive and parse a packet
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                SocketAddress senderAddr = packet.getSocketAddress();
                RemotePacket inboundMessage = (RemotePacket) fromBinary(buf);
                // 2. Prepare return
                RemotePacket outboundMessage = this.handle(inboundMessage);
                if (outboundMessage == null) {
                    continue;
                }
                // 3. New packet and send
                byte[] outputBinary = toBinary(outboundMessage);
                this.socket.send(new DatagramPacket(outputBinary, outputBinary.length, senderAddr));
            } catch (IOException e) {
                logger.error("Error when receiving message.");
            }
        }

    }

    private void send() {
        try {
            while (!Thread.interrupted()) {
                try {
                    HeartBeatPacket heartBeat = new HeartBeatPacket(isRunning ? "Running" : "Stopping");
                    RemotePacket message = new RemotePacket(crawler.getCrawlerIdentifier(), heartBeat, String.valueOf(System.nanoTime()));
                    byte[] outputBinary = toBinary(message);
                    this.socket.send(new DatagramPacket(outputBinary, outputBinary.length, this.masterSocketAddr));
                } catch (IOException e) {
                    e.printStackTrace();
                }

                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            logger.error("Error when sending message.");
        }
    }

    public void sendUrls(int nodeId, List<String> urls) {
        try {
            // 1. Get destination
            InetSocketAddress dest = this.addrs.get(nodeId);
            // 2. Prepare payload
            String[] input = new String[urls.size()];
            urls.toArray(input);
            URLListPacket urlListPacket = new URLListPacket(input);
            RemotePacket message = new RemotePacket(crawler.getCrawlerIdentifier(), urlListPacket, String.valueOf(System.nanoTime()));
            byte[] outputBinary = toBinary(message);
            DatagramPacket responsePacket = new DatagramPacket(outputBinary, outputBinary.length, dest);
            // 3. Send packet
            this.socket.send(responsePacket);
        } catch (IOException e) {
            logger.error("Error when sending urls.");
        }
    }

    public final List<String> getNames() {
        return names;
    }

    public void stop() {
        if (this.masterSocketAddr == null) {
            return;
        }
        this.isRunning = false;
        try {
            Thread.sleep(5 * 1000); // To assure the next heart beat to be sent out
        } catch (InterruptedException e) {
        }
        this.sender.interrupt();
        this.receiver.interrupt();
    }

}