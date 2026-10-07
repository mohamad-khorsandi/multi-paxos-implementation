package org.example;


import io.grpc.*;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ServerMain {
    static ProposerAgent proposerAgent;
    static AcceptorAgent acceptorAgent;
    static LearnerAgent learnerAgent;
    static SequenceManagerAgent sequenceManagerAgent;

    public static void main(String[] args) throws IOException, InterruptedException {
        int selfPortNumber = Integer.parseInt(args[0]);
        int replicaNumber = Integer.parseInt(args[1]);
        List<Integer> peerPortNumbers = List.of(Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));

        ArrayList<AcceptorServiceGrpc.AcceptorServiceBlockingStub> acceptorStubs = new ArrayList<>();
        ArrayList<LearnerServiceGrpc.LearnerServiceBlockingStub> learnerStubs = new ArrayList<>();
        for (int port : peerPortNumbers) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
            acceptorStubs.add(AcceptorServiceGrpc.newBlockingStub(channel));
            learnerStubs.add(LearnerServiceGrpc.newBlockingStub(channel));
        }

        proposerAgent = new ProposerAgent(acceptorStubs, replicaNumber);
        acceptorAgent = new AcceptorAgent(learnerStubs, replicaNumber);
        learnerAgent = new LearnerAgent(acceptorStubs);
        sequenceManagerAgent = new SequenceManagerAgent();

        Server server = ServerBuilder.forPort(selfPortNumber)
                .addService(sequenceManagerAgent)
                .addService(acceptorAgent)
                .addService(learnerAgent)
                .addService(ProtoReflectionService.newInstance())
                .build()
                .start();

        System.out.println("Server running on port "+ selfPortNumber + "....");
        server.awaitTermination();
    }
}