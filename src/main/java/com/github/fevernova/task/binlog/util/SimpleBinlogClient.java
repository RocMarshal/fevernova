package com.github.fevernova.task.binlog.util;


import com.github.fevernova.framework.common.FNException;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.ChecksumType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.RotateEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.network.AuthenticationException;
import com.github.shyiko.mysql.binlog.network.ServerException;
import com.github.shyiko.mysql.binlog.network.protocol.*;
import com.github.shyiko.mysql.binlog.network.protocol.command.AuthenticateCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.DumpBinaryLogCommand;
import com.github.shyiko.mysql.binlog.network.protocol.command.QueryCommand;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


@Slf4j
public class SimpleBinlogClient {


    private static final int MAX_PACKET_LENGTH = 16777215;

    private static final int SOCKET_TIMEOUT = 5000;

    private final String hostname;

    private final int port;

    private final String schema;

    private final String username;

    private final String password;

    private long serverId;

    private long fetchTime;

    private EventDeserializer eventDeserializer = new EventDeserializer();

    private PacketChannel channel;

    @Getter
    private String binlogFileName;

    @Getter
    private long binlogPosition = 4;

    @Getter
    private long lastDBTime;

    private String tmpFileName;

    private long tmpEnd = 0;

    private long tmpPosition = 0;


    public SimpleBinlogClient(String hostname, int port, String username, String pwd, long serverId, long fetchTime) {

        this.hostname = hostname;
        this.port = port;
        this.schema = null;
        this.username = username;
        this.password = pwd;
        this.serverId = serverId;
        this.fetchTime = fetchTime;
    }


    public void connect() throws Exception {

        ensureEventDataDeserializer(EventType.ROTATE, RotateEventDataDeserializer.class);
        try {
            for (Pair<String, Long> file : getAllBinlogFilesAndSize()) {
                log.info("auto fetch binlog file : " + file.getLeft());
                openChannel(SOCKET_TIMEOUT);
                checkChannel();
                this.tmpFileName = file.getLeft();
                this.tmpEnd = file.getRight();
                this.channel.write(new DumpBinaryLogCommand(this.serverId, file.getLeft(), 4));
                listenForEventPackets();
                disconnectChannel();
                if (this.binlogFileName != null) {
                    log.info("auto fetch bingo !");
                    break;
                }
            }
        } finally {
            disconnectChannel();
        }
    }


    private void listenForEventPackets() throws IOException {

        ByteArrayInputStream inputStream = this.channel.getInputStream();
        while (inputStream.peek() != -1) {
            int packetLength = inputStream.readInteger(3);
            inputStream.skip(1); // 1 byte for sequence
            int marker = inputStream.read();
            if (marker == 0xFF) {
                ErrorPacket errorPacket = new ErrorPacket(inputStream.read(packetLength - 1));
                throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(), errorPacket.getSqlState());
            }
            if (marker == 0xFE) {
                break;
            }
            Event event = eventDeserializer.nextEvent(packetLength == MAX_PACKET_LENGTH ? new ByteArrayInputStream
                    (readPacketSplitInChunks(inputStream, packetLength - 1)) : inputStream);

            if (event.getData() instanceof EventDeserializer.EventDataWrapper) {
                event = new Event(event.getHeader(), ((EventDeserializer.EventDataWrapper) event.getData()).getExternal());
            }

            this.tmpPosition = ((EventHeaderV4) event.getHeader()).getNextPosition();

            if (this.tmpPosition == 0) {
                continue;
            }

            if (this.tmpPosition >= this.tmpEnd || event.getHeader().getEventType() == EventType.ROTATE || event.getHeader()
                                                                                                                   .getEventType()
                                                                                                           == EventType.STOP) {
                break;
            }

            //found == null 没有找到xid类型的数据, false 找到了xid 但是eventtime比fetchtime大 true eventtime比fetchtime小
            Boolean found = null;
            if (event.getHeader().getEventType() == EventType.XID) {
                found = event.getHeader().getTimestamp() < this.fetchTime;
            }
            this.lastDBTime = event.getHeader().getTimestamp();

            if (found == null) {
                continue;
            }

            if (found) {
                if (log.isDebugEnabled()) {
                    log.debug("maybe " + this.binlogFileName + "/" + this.binlogPosition);
                }
                this.binlogFileName = this.tmpFileName;
                this.binlogPosition = ((EventHeaderV4) event.getHeader()).getPosition();
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("fetch to " + this.binlogFileName + "/" + this.binlogPosition);
                }
                break;
            }
        }

    }


    private List<Pair<String, Long>> getAllBinlogFilesAndSize() throws Exception {

        List<Pair<String, Long>> result = Lists.newArrayList();
        try {
            ResultSetRowPacket[] resultSet = getResultSetRowPackets("show master logs");
            for (ResultSetRowPacket packet : resultSet) {
                result.add(Pair.of(packet.getValue(0), Long.parseLong(packet.getValue(1))));
            }
        } catch (Exception e) {
            log.info("use show master status");
            ResultSetRowPacket[] firstLogEventsSet = getResultSetRowPackets("show binlog events limit 1");
            String firstLogFileName = firstLogEventsSet[0].getValue(0);
            ResultSetRowPacket[] lastLogNameSet = getResultSetRowPackets("show master status");
            String lastLogFileName = lastLogNameSet[0].getValue(0);

            int splitIndex = firstLogFileName.lastIndexOf(".");
            String firstLogFilePrefix = firstLogFileName.substring(0, splitIndex + 1);
            String lastLogFilePrefix = lastLogFileName.substring(0, splitIndex + 1);
            if (!firstLogFilePrefix.equals(lastLogFilePrefix)) {
                throw new FNException("Failed to determine binlog filename/position, cause by : the name of binLogFile " +
                                      "is't unified");
            }
            int firstSuffix = Integer.parseInt((firstLogFileName.substring(splitIndex + 1)));
            int lastSuffix = Integer.parseInt(lastLogFileName.substring(splitIndex + 1));
            while (firstSuffix < lastSuffix) {
                result.add(Pair.of(lastLogFilePrefix + String.format("%06d", firstSuffix), Long.MAX_VALUE));
                firstSuffix++;
            }
            result.add(Pair.of(lastLogFileName, Long.parseLong(lastLogNameSet[0].getValue(1))));
        }
        if (result.isEmpty()) {
            throw new FNException("Failed to determine binlog filename/position");
        }
        result.sort((o1, o2) -> o2.getLeft().compareTo(o1.getLeft()));
        return result;
    }


    private ResultSetRowPacket[] getResultSetRowPackets(String sql) throws Exception {

        openChannel(SOCKET_TIMEOUT);
        checkChannel();
        this.channel.write(new QueryCommand(sql));
        ResultSetRowPacket[] rowPackets = readResultSet();
        if (rowPackets.length == 0) {
            throw new IOException("Failed to determine binlog filename/position with sql : " + sql);
        }
        disconnectChannel();
        return rowPackets;
    }


    private void ensureEventDataDeserializer(EventType eventType, Class<? extends EventDataDeserializer>
            eventDataDeserializerClass) {

        EventDataDeserializer eventDataDeserializer = eventDeserializer.getEventDataDeserializer(eventType);
        if (eventDataDeserializer.getClass() != eventDataDeserializerClass && eventDataDeserializer.getClass() !=
                                                                              EventDeserializer.EventDataWrapper.Deserializer.class) {
            EventDataDeserializer internalEventDataDeserializer;
            try {
                internalEventDataDeserializer = eventDataDeserializerClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            eventDeserializer.setEventDataDeserializer(eventType, new EventDeserializer.EventDataWrapper.Deserializer
                    (internalEventDataDeserializer, eventDataDeserializer));
        }
    }


    private byte[] readPacketSplitInChunks(ByteArrayInputStream inputStream, int packetLength) throws IOException {

        byte[] result = inputStream.read(packetLength);
        int chunkLength;
        do {
            chunkLength = inputStream.readInteger(3);
            inputStream.skip(1); // 1 byte for sequence
            result = Arrays.copyOf(result, result.length + chunkLength);
            inputStream.fill(result, result.length - chunkLength, chunkLength);
        } while (chunkLength == Packet.MAX_LENGTH);
        return result;
    }


    // Channel 相关的操作
    private void openChannel(int timeout) throws Exception {

        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(this.hostname, this.port), timeout);
            this.channel = new PacketChannel(socket);
            if (this.channel.getInputStream().peek() == -1) {
                throw new EOFException();
            }
        } catch (IOException e) {
            throw new IOException("Failed to connect to MySQL on " + this.hostname + ":" + this.port + ". Please make" + " " +
                                  "sure" + " it's" + " running.", e);
        }
    }


    private void checkChannel() throws Exception {

        try {
            GreetingPacket greetingPacket = receiveGreeting();
            authenticate(greetingPacket);
            ChecksumType checksumType = fetchBinlogChecksum();
            if (checksumType != ChecksumType.NONE) {
                confirmSupportOfChecksum(checksumType);
            }
        } catch (IOException e) {
            throw new IOException("Failed to connect to MySQL on " + this.hostname + ":" + this.port + ". Please make" + " " +
                                  "sure" + " it's authenticated.", e);
        }
    }


    private void disconnectChannel() throws IOException {

        if (this.channel != null && this.channel.isOpen()) {
            this.channel.close();
        }
    }


    private GreetingPacket receiveGreeting() throws IOException {

        byte[] initialHandshakePacket = this.channel.read();
        if (initialHandshakePacket[0] == (byte) 0xFF) {
            byte[] bytes = Arrays.copyOfRange(initialHandshakePacket, 1, initialHandshakePacket.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(), errorPacket.getSqlState());
        }
        return new GreetingPacket(initialHandshakePacket);
    }


    private void authenticate(GreetingPacket greetingPacket) throws IOException {

        int collation = greetingPacket.getServerCollation();
        int packetNumber = 1;
        AuthenticateCommand authenticateCommand = new AuthenticateCommand(this.schema, this.username, this.password,
                                                                          greetingPacket.getScramble());
        authenticateCommand.setCollation(collation);
        this.channel.write(authenticateCommand, packetNumber);
        byte[] authenticationResult = this.channel.read();
        if (authenticationResult[0] != (byte) 0x00 /* ok */) {
            if (authenticationResult[0] == (byte) 0xFF /* error */) {
                byte[] bytes = Arrays.copyOfRange(authenticationResult, 1, authenticationResult.length);
                ErrorPacket errorPacket = new ErrorPacket(bytes);
                throw new AuthenticationException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(), errorPacket
                        .getSqlState());
            }
            throw new AuthenticationException("Unexpected authentication result (" + authenticationResult[0] + ")");
        }
    }


    private ChecksumType fetchBinlogChecksum() throws IOException {

        this.channel.write(new QueryCommand("show global variables like 'binlog_checksum'"));
        ResultSetRowPacket[] resultSet = readResultSet();
        if (resultSet.length == 0) {
            return ChecksumType.NONE;
        }
        return ChecksumType.valueOf(resultSet[0].getValue(1).toUpperCase());
    }


    private void confirmSupportOfChecksum(ChecksumType checksumType) throws IOException {

        channel.write(new QueryCommand("set @master_binlog_checksum= @@global.binlog_checksum"));
        byte[] statementResult = channel.read();
        if (statementResult[0] == (byte) 0xFF /* error */) {
            byte[] bytes = Arrays.copyOfRange(statementResult, 1, statementResult.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(), errorPacket.getSqlState());
        }
        eventDeserializer.setChecksumType(checksumType);
    }


    private ResultSetRowPacket[] readResultSet() throws IOException {

        List<ResultSetRowPacket> resultSet = new LinkedList<ResultSetRowPacket>();
        byte[] statementResult = channel.read();
        if (statementResult[0] == (byte) 0xFF /* error */) {
            byte[] bytes = Arrays.copyOfRange(statementResult, 1, statementResult.length);
            ErrorPacket errorPacket = new ErrorPacket(bytes);
            throw new ServerException(errorPacket.getErrorMessage(), errorPacket.getErrorCode(), errorPacket.getSqlState());
        }
        while ((channel.read())[0] != (byte) 0xFE /* eof */) { /* skip */ }
        for (byte[] bytes; (bytes = channel.read())[0] != (byte) 0xFE /* eof */; ) {
            resultSet.add(new ResultSetRowPacket(bytes));
        }
        return resultSet.toArray(new ResultSetRowPacket[resultSet.size()]);
    }
}
