package com.bright.raft.admin;

import com.bright.raft.proto.RaftProto;
import com.bright.raft.service.RaftClientService;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.List;

public class AdminMain {
    private static final JsonFormat jsonFormat = new JsonFormat();

    public static void main(String[] args) {
        // parse args
        if (args.length < 3) {
            System.out.println("java -jar AdminMain servers cmd subCmd [args]");
            System.exit(1);
        }
        // servers format is like "10.1.1.1:8010:1,10.2.2.2:8011:2,10.3.3.3.3:8012:3"
        String servers = args[0];
        String cmd = args[1];
        String subCmd = args[2];
        Validate.isTrue(cmd.equals("conf"));
        Validate.isTrue(subCmd.equals("get")
                || subCmd.equals("add")
                || subCmd.equals("del"));
        RaftClientServiceProxy client = new RaftClientServiceProxy(servers);
        switch (subCmd) {
            case "get": {
                RaftProto.GetConfigurationRequest request = RaftProto.GetConfigurationRequest.newBuilder().build();
                RaftProto.GetConfigurationResponse response = client.getConfiguration(request);
                if (response != null) {
                    System.out.println(jsonFormat.printToString(response));
                } else {
                    System.out.print("response == null");
                }

                break;
            }
            case "add": {
                List<RaftProto.Server> serverList = parseServers(args[3]);
                RaftProto.AddPeersRequest request = RaftProto.AddPeersRequest.newBuilder()
                        .addAllServers(serverList).build();
                RaftProto.AddPeersResponse response = client.addPeers(request);
                if (response != null) {
                    System.out.println(response.getResCode());
                } else {
                    System.out.print("response == null");
                }
                break;
            }
            case "del": {
                List<RaftProto.Server> serverList = parseServers(args[3]);
                RaftProto.RemovePeersRequest request = RaftProto.RemovePeersRequest.newBuilder()
                        .addAllServers(serverList).build();
                RaftProto.RemovePeersResponse response = client.removePeers(request);
                if (response != null) {
                    System.out.println(response.getResCode());
                } else {
                    System.out.print("response == null");
                }
                break;
            }
        }
        client.stop();
    }

    private static List<RaftProto.Server> parseServers(String serversString) {
        List<RaftProto.Server> serverList = new ArrayList<>();
        String[] splitArray1 = serversString.split(",");
        for (String addr : splitArray1) {
            String[] splitArray2 = addr.split(":");
            RaftProto.Endpoint endPoint = RaftProto.Endpoint.newBuilder()
                    .setHost(splitArray2[0])
                    .setPort(Integer.parseInt(splitArray2[1])).build();
            RaftProto.Server server = RaftProto.Server.newBuilder()
                    .setEndpoint(endPoint)
                    .setServerId(Integer.parseInt(splitArray2[2])).build();
            serverList.add(server);
        }
        return serverList;
    }
}
