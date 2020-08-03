package com.bright.raft;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.instance.Endpoint;
import com.bright.raft.proto.RaftProto;
import com.bright.raft.service.RaftConsensusServiceAsync;

public class Peer {
    private RaftProto.Server server;
    private RpcClient rpcClient;
    private RaftConsensusServiceAsync raftConsensusServiceAsync;
    // 需要发送给follower的下一个日志条目的索引值，只对leader有效
    private long nextIndex;
    // 已复制日志的最高索引值
    private long matchIndex;
    // 候选人赢得此张选票时为true
    private volatile Boolean voteGranted;
    // 跟随者和leader的日志index是否相近，即跟随者是否能紧随leader
    private volatile boolean isCatchUp;

    public Peer(RaftProto.Server server) {
        this.server = server;
        this.rpcClient = new RpcClient(new Endpoint(
                server.getEndpoint().getHost(),
                server.getEndpoint().getPort()));
        raftConsensusServiceAsync = BrpcProxy.getProxy(rpcClient, RaftConsensusServiceAsync.class);
        isCatchUp = false;
    }

    public RaftProto.Server getServer() {
        return server;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }

    public RaftConsensusServiceAsync getRaftConsensusServiceAsync() {
        return raftConsensusServiceAsync;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    public Boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(Boolean voteGranted) {
        this.voteGranted = voteGranted;
    }


    public boolean isCatchUp() {
        return isCatchUp;
    }

    public void setCatchUp(boolean catchUp) {
        isCatchUp = catchUp;
    }
}
