package com.bright.raft.service.impl;

import com.bright.raft.RaftNode;
import com.bright.raft.proto.RaftProto;
import com.bright.raft.service.RaftConsensusService;
import com.bright.raft.util.ConfigurationUtils;
import com.bright.raft.util.RaftFileUtils;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

/**
 * raft节点之间相互通信
 */
public class RaftConsensusServiceImpl implements RaftConsensusService {

    private static final Logger logger = LoggerFactory.getLogger(RaftConsensusServiceImpl.class);
    private static final JsonFormat PRINTER = new JsonFormat();

    private RaftNode raftNode;

    public RaftConsensusServiceImpl(RaftNode node) {
        this.raftNode = node;
    }

    @Override
    public RaftProto.VoteResponse preVote(RaftProto.VoteRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
            responseBuilder.setGranted(false);
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            // 如果集群配置没有 请求选票的候选人的 Id，直接返回
            if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), request.getServerId())) {
                return responseBuilder.build();
            }
            // 如果 候选人的任期号 < 当前节点的任期号，直接返回
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            // 只有候选人的日志较本节点更新，才可以获得选票
            boolean isLogOk = request.getLastLogTerm() > raftNode.getLastLogTerm()
                    || (request.getLastLogTerm() == raftNode.getLastLogTerm()
                    && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
            if (!isLogOk) {
                return responseBuilder.build();
            } else {
                // 候选人获得选票，更新本节点的任期号
                responseBuilder.setGranted(true);
                responseBuilder.setTerm(raftNode.getCurrentTerm());
            }
            logger.info("preVote request from server {} in term {} (my term is {}), granted={}",
                    request.getServerId(), request.getTerm(), raftNode.getCurrentTerm(), responseBuilder.getGranted());
            return responseBuilder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.VoteResponse requestVote(RaftProto.VoteRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
            responseBuilder.setGranted(false);
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            // 如果集群配置没有 请求选票的候选人的 Id，直接返回
            if (!ConfigurationUtils.containsServer(raftNode.getConfiguration(), request.getServerId())) {
                return responseBuilder.build();
            }
            // 如果 候选人的任期号 < 当前节点的任期号，直接返回
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            // 如果 候选人的任期号 > 当前节点的任期号，更新本节点的任期号为 候选人的任期号
            if (request.getTerm() > raftNode.getCurrentTerm()) {
                raftNode.stepDown(request.getTerm());
            }
            // 只有候选人的日志较本节点更新，才可以获得选票
            boolean logIsOk = request.getLastLogTerm() > raftNode.getLastLogTerm()
                    || (request.getLastLogTerm() == raftNode.getLastLogTerm()
                    && request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
            if (raftNode.getVotedFor() == 0 && logIsOk) {
                raftNode.stepDown(request.getTerm());
                raftNode.setVotedFor(request.getServerId());
                raftNode.getRaftLog().updateMetaData(raftNode.getCurrentTerm(), raftNode.getVotedFor(), null);
                responseBuilder.setGranted(true);
                responseBuilder.setTerm(raftNode.getCurrentTerm());
            }
            logger.info("RequestVote request from server {} in term {} (my term is {}), granted={}",
                    request.getServerId(), request.getTerm(), raftNode.getCurrentTerm(), responseBuilder.getGranted());
            return responseBuilder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.AppendEntriesResponse.Builder responseBuilder
                    = RaftProto.AppendEntriesResponse.newBuilder();
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);
            responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());
            // 如果领导人的term < currentTerm，返回false
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            // 只有从节点会收到这个请求
            raftNode.stepDown(request.getTerm());
            if (raftNode.getLeaderId() == 0) {
                raftNode.setLeaderId(request.getServerId());
                logger.info("new leaderId={}, conf={}", raftNode.getLeaderId(), PRINTER.printToString(raftNode.getConfiguration()));
            }
            //
            if (raftNode.getLeaderId() != request.getServerId()) {
                logger.warn("Another peer={} declares that it is the leader at term={} which was occupied by leader={}",
                        request.getServerId(), request.getTerm(), raftNode.getLeaderId());
                raftNode.stepDown(request.getTerm() + 1);
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);
                responseBuilder.setTerm(request.getTerm() + 1);
                return responseBuilder.build();
            }

            // 如果跟随者在它的日志中找不到包含相同索引位置和任期号的条目，那么他就会拒绝接收新的日志条目
            if (request.getPrevLogIndex() > raftNode.getRaftLog().getLastLogIndex()) {
                logger.info("Rejecting AppendEntries RPC would leave gap, request prevLogIndex={}, my lastLogIndex={}",
                        request.getPrevLogIndex(), raftNode.getRaftLog().getLastLogIndex());
                return responseBuilder.build();
            }
            // 如果日志在prevLogIndex 位置处的日志条目的任期号和 请求的prevLogTerm不匹配，返回false
            if (request.getPrevLogIndex() >= raftNode.getRaftLog().getFirstLogIndex()
                    && raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex()) != request.getPrevLogTerm()) {
                logger.info("Rejecting AppendEntries RPC: terms don't agree, request prevLogTerm={} in prevLogIndex={}, my is {}",
                        request.getPrevLogTerm(), request.getPrevLogIndex(), raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex()));
                Validate.isTrue(request.getPrevLogIndex() > 0);
                // 如果已经存在的日志条目和新的产生冲突（索引号相同但是任期号不同），删除这一条和之后所有的
                responseBuilder.setLastLogIndex(request.getPrevLogIndex() - 1);
                return responseBuilder.build();
            }

            // 日志为空，代表心跳
            if (request.getEntriesCount() == 0) {
                logger.debug("heartbeat request from peer={} at term={}, my term={}",
                        request.getServerId(), request.getTerm(), raftNode.getCurrentTerm());
                responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
                responseBuilder.setTerm(raftNode.getCurrentTerm());
                responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());
                advanceCommitIndex(request);
                return responseBuilder.build();
            }

            // 文本data日志
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
            List<RaftProto.LogEntry> entries = new ArrayList<>();
            long index = request.getPrevLogIndex();
            for (RaftProto.LogEntry entry : request.getEntriesList()) {
                index++;
                if (index < raftNode.getRaftLog().getFirstLogIndex()) {
                    continue;
                }
                if (raftNode.getRaftLog().getLastLogIndex() >= index) {
                    if (raftNode.getRaftLog().getEntryTerm(index) == entry.getTerm()) {
                        continue;
                    }
                    // truncate segment log from index
                    long lastIndexKept = index - 1;
                    // 如果已经存在的日志条目和新的产生冲突（索引号相同但是任期号不同），删除这一条和之后所有的
                    raftNode.getRaftLog().truncateSuffix(lastIndexKept);
                }
                entries.add(entry);
            }
            raftNode.getRaftLog().append(entries);
            raftNode.getRaftLog().updateMetaData(raftNode.getCurrentTerm(),
                    null, raftNode.getRaftLog().getFirstLogIndex());
            responseBuilder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());

            advanceCommitIndex(request);
            logger.info("AppendEntries request from server {} in term {} (my term is {}), entryCount={} resCode={}",
                    request.getServerId(), request.getTerm(), raftNode.getCurrentTerm(), request.getEntriesCount(), responseBuilder.getResCode());
            return responseBuilder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request) {
        RaftProto.InstallSnapshotResponse.Builder responseBuilder
                = RaftProto.InstallSnapshotResponse.newBuilder();
        responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_FAIL);

        raftNode.getLock().lock();
        try {
            responseBuilder.setTerm(raftNode.getCurrentTerm());
            // 如果term < currentTerm就立即回复
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return responseBuilder.build();
            }
            raftNode.stepDown(request.getTerm());
            if (raftNode.getLeaderId() == 0) {
                raftNode.setLeaderId(request.getServerId());
                logger.info("new leaderId={}, conf={}",
                        raftNode.getLeaderId(),
                        PRINTER.printToString(raftNode.getConfiguration()));
            }
        } finally {
            raftNode.getLock().unlock();
        }

        if (raftNode.getSnapshot().getIsTakeSnapshot().get()) {
            logger.warn("already in take snapshot, do not handle install snapshot request now");
            return responseBuilder.build();
        }

        raftNode.getSnapshot().getIsInstallSnapshot().set(true);
        RandomAccessFile randomAccessFile = null;
        raftNode.getSnapshot().getLock().lock();
        try {
            // write snapshot data to local
            String tmpSnapshotDir = raftNode.getSnapshot().getSnapshotDir() + ".tmp";
            File file = new File(tmpSnapshotDir);
            if (request.getIsFirst()) {
                if (file.exists()) {
                    file.delete();
                }
                file.mkdir();
                logger.info("begin accept install snapshot request from serverId={}", request.getServerId());
                raftNode.getSnapshot().updateMetaData(tmpSnapshotDir,
                        request.getSnapshotMetaData().getLastIncludedIndex(),
                        request.getSnapshotMetaData().getLastIncludedTerm(),
                        request.getSnapshotMetaData().getConfiguration());
            }
            // write to file
            String currentDataDirName = tmpSnapshotDir + File.separator + "data";
            File currentDataDir = new File(currentDataDirName);
            if (!currentDataDir.exists()) {
                currentDataDir.mkdirs();
            }

            String currentDataFileName = currentDataDirName + File.separator + request.getFileName();
            File currentDataFile = new File(currentDataFileName);
            // 文件名可能是个相对路径，比如topic/0/message.txt
            if (!currentDataFile.getParentFile().exists()) {
                currentDataFile.getParentFile().mkdirs();
            }
            if (!currentDataFile.exists()) {
                currentDataFile.createNewFile();
            }
            randomAccessFile = RaftFileUtils.openFile(
                    tmpSnapshotDir + File.separator + "data",
                    request.getFileName(), "rw");
            randomAccessFile.seek(request.getOffset());
            randomAccessFile.write(request.getData().toByteArray());
            // move tmp dir to snapshot dir if this is the last package
            if (request.getIsLast()) {
                File snapshotDirFile = new File(raftNode.getSnapshot().getSnapshotDir());
                if (snapshotDirFile.exists()) {
                    FileUtils.deleteDirectory(snapshotDirFile);
                }
                FileUtils.moveDirectory(new File(tmpSnapshotDir), snapshotDirFile);
            }
            responseBuilder.setResCode(RaftProto.ResCode.RES_CODE_SUCCESS);
            logger.info("install snapshot request from server {} " +
                            "in term {} (my term is {}), resCode={}",
                    request.getServerId(), request.getTerm(),
                    raftNode.getCurrentTerm(), responseBuilder.getResCode());
        } catch (IOException ex) {
            logger.warn("when handle installSnapshot request, meet exception:", ex);
        } finally {
            RaftFileUtils.closeFile(randomAccessFile);
            raftNode.getSnapshot().getLock().unlock();
        }

        if (request.getIsLast() && responseBuilder.getResCode() == RaftProto.ResCode.RES_CODE_SUCCESS) {
            // apply state machine
            // TODO: make this async
            String snapshotDataDir = raftNode.getSnapshot().getSnapshotDir() + File.separator + "data";
            raftNode.getStateMachine().readSnapshot(snapshotDataDir);
            long lastSnapshotIndex;
            // 重新加载snapshot
            raftNode.getSnapshot().getLock().lock();
            try {
                raftNode.getSnapshot().reload();
                lastSnapshotIndex = raftNode.getSnapshot().getMetaData().getLastIncludedIndex();
            } finally {
                raftNode.getSnapshot().getLock().unlock();
            }

            // discard old log entries
            raftNode.getLock().lock();
            try {
                raftNode.getRaftLog().truncatePrefix(lastSnapshotIndex + 1);
            } finally {
                raftNode.getLock().unlock();
            }
            logger.info("end accept install snapshot request from serverId={}", request.getServerId());
        }

        if (request.getIsLast()) {
            raftNode.getSnapshot().getIsInstallSnapshot().set(false);
        }

        return responseBuilder.build();
    }

    // in lock, for follower
    private void advanceCommitIndex(RaftProto.AppendEntriesRequest request) {
        // 如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
        long newCommitIndex = Math.min(request.getCommitIndex(),
                request.getPrevLogIndex() + request.getEntriesCount());
        raftNode.setCommitIndex(newCommitIndex);
        // 应用到 状态机
        if (raftNode.getLastAppliedIndex() < raftNode.getCommitIndex()) {
            // apply state machine
            for (long index = raftNode.getLastAppliedIndex() + 1;
                 index <= raftNode.getCommitIndex(); index++) {
                RaftProto.LogEntry entry = raftNode.getRaftLog().getEntry(index);
                if (entry != null) {
                    if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_DATA) {   // 日志
                        raftNode.getStateMachine().apply(entry.getData().toByteArray());
                    } else if (entry.getType() == RaftProto.EntryType.ENTRY_TYPE_CONFIGURATION) {   // 集群配置
                        raftNode.applyConfiguration(entry);
                    }
                }
                raftNode.setLastAppliedIndex(index);
            }
        }
    }

}
