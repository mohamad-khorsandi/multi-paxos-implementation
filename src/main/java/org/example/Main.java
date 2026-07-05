package org.example;


import io.grpc.*;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        int selfPortNumber = Integer.parseInt(args[0]);
        int proposalOffset = Integer.parseInt(args[1]);
        List<Integer> peerPortNumbers = List.of(Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        Server server = ServerBuilder.forPort(selfPortNumber)
                .addService(new AcceptorAgent())
                .addService(new LearnerAgent())
                .addService(new ProposerAgent(proposalOffset, peerPortNumbers))
                .addService(ProtoReflectionService.newInstance())
                .build()
                .start();

        System.out.println("Server running on port "+ selfPortNumber + "....");
        server.awaitTermination();
    }
}