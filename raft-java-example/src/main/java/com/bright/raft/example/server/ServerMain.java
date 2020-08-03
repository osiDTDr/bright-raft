package com.bright.raft.example.server;

import com.baidu.brpc.server.RpcServer;
import com.bright.raft.RaftOptions;
import com.bright.raft.example.server.service.ExampleService;
import com.bright.raft.RaftNode;
import com.bright.raft.example.server.service.impl.ExampleServiceImpl;
import com.bright.raft.proto.RaftProto;
import com.bright.raft.service.RaftClientService;
import com.bright.raft.service.RaftConsensusService;
import com.bright.raft.service.impl.RaftClientServiceImpl;
import com.bright.raft.service.impl.RaftConsensusServiceImpl;

import java.util.ArrayList;
import java.util.List;


public class ServerMain {
    public static void main(String[] args) {
        // TODO: 2020/7/31 修改RPC框架
        if (args.length != 3) {
            System.out.print("Usage: ./run_server.sh DATA_PATH CLUSTER CURRENT_NODE\n");
            System.exit(-1);
        }
        // parse args
        // raft data dir
        String dataPath = args[0];
        // peers, format is "host:port:serverId,host2:port2:serverId2"
        String servers = args[1];
        String[] splitArray = servers.split(",");
        List<RaftProto.Server> serverList = new ArrayList<>();
        for (String serverString : splitArray) {
            RaftProto.Server server = parseServer(serverString);
            serverList.add(server);
        }
        // local server
        RaftProto.Server localServer = parseServer(args[2]);

        // initialize RPCServer
        RpcServer server = new RpcServer(localServer.getEndpoint().getPort());
        // 设置Raft选项
        RaftOptions raftOptions = new RaftOptions();
        raftOptions.setDataDir(dataPath);
        raftOptions.setSnapshotMinLogSize(10 * 1024);
        raftOptions.setSnapshotPeriodSeconds(30);
        raftOptions.setMaxSegmentFileSize(1024 * 1024);
        // 应用状态机
        ExampleStateMachine stateMachine = new ExampleStateMachine(raftOptions.getDataDir());
        // 初始化RaftNode
        RaftNode raftNode = new RaftNode(raftOptions, serverList, localServer, stateMachine);
        // 注册Raft节点之间相互调用的服务
        RaftConsensusService raftConsensusService = new RaftConsensusServiceImpl(raftNode);
        server.registerService(raftConsensusService);
        // 注册给Client调用的Raft服务
        RaftClientService raftClientService = new RaftClientServiceImpl(raftNode);
        server.registerService(raftClientService);
        // 注册应用自己提供的服务
        ExampleService exampleService = new ExampleServiceImpl(raftNode, stateMachine);
        server.registerService(exampleService);
        // 启动RPCServer，初始化Raft节点
        server.start();
        // 开启raft节点
        raftNode.init();
    }

    /**
     * @param serverString e.g. 127.0.0.1:8051:1
     * @return protobuf 生成的 Server
     */
    private static RaftProto.Server parseServer(String serverString) {
        String[] splitServer = serverString.split(":");
        // e.g 127.0.0.1
        String host = splitServer[0];
        // e.g 8051
        Integer port = Integer.parseInt(splitServer[1]);
        // e.g 1
        int serverId = Integer.parseInt(splitServer[2]);
        RaftProto.Endpoint endPoint = RaftProto.Endpoint.newBuilder()
                .setHost(host).setPort(port).build();
        RaftProto.Server.Builder serverBuilder = RaftProto.Server.newBuilder();
        return serverBuilder.setServerId(serverId).setEndpoint(endPoint).build();
    }
}
