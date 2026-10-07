package org.example;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.util.ArrayList;
import java.util.List;

public class ClientMain {
    public static void main(String[] args) {
        List<Integer> peerPortNumbers = List.of(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        ArrayList<SequenceManagerServiceGrpc.SequenceManagerServiceBlockingStub> sequenceManagerStubs = new ArrayList<>();
        for (int port : peerPortNumbers) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
            sequenceManagerStubs.add(SequenceManagerServiceGrpc.newBlockingStub(channel));
        }


        String operation = args[3];
        if (operation.equals("add_command")) {
            String command = args[4];
            addCommand(sequenceManagerStubs, command);
        } else if (operation.equals("list_command")) {
            listCommand(sequenceManagerStubs);
        } else
            throw new RuntimeException("command should be either \"add_command\" or \"list_command\"");


    }

    static void addCommand(ArrayList<SequenceManagerServiceGrpc.SequenceManagerServiceBlockingStub> sequenceManagerStubs, String command) {
        int stubIdx = 0;
        AddCmdRequest addCmdRequest = AddCmdRequest.newBuilder().setValue(command).build();
        SequenceManagerServiceGrpc.SequenceManagerServiceBlockingStub curStub = sequenceManagerStubs.get(stubIdx);

        while (true) {
            try {
                AddCmdResponse addCmdResponse = curStub.addCommand(addCmdRequest);
                if (addCmdResponse.getStatus().equals(AddCmdResponse.Status.SUCCESS)) {
                    System.out.println("CLIENT: The command \"" + command + "\" has been chosen in sequence number " + addCmdResponse.getCommandNumber());
                    return;
                }
            } catch (Exception e) {
                System.out.println("CLIENT: The current server is down, switching to the next one.");
                stubIdx = getNextStubIdx(stubIdx);
                curStub = sequenceManagerStubs.get(stubIdx);
            }
        }
    }

    static void listCommand(ArrayList<SequenceManagerServiceGrpc.SequenceManagerServiceBlockingStub> sequenceManagerStubs) {
        int stubIdx = 0;
        ListCmdRequest listCmdRequest = ListCmdRequest.newBuilder().build();
        SequenceManagerServiceGrpc.SequenceManagerServiceBlockingStub curStub = sequenceManagerStubs.get(stubIdx);

        while (true) {
            try {
                ListCmdResponse listCmdResponse = curStub.listCommand(listCmdRequest);
                System.out.println("CLIENT: This is the sequence so far: " + listCmdResponse.getCommandsList());
                return;
            } catch (Exception e) {
                System.out.println("CLIENT: The current server is down, switching to the next one.");
                stubIdx = getNextStubIdx(stubIdx);
                curStub = sequenceManagerStubs.get(stubIdx);
            }
        }
    }

    static int getNextStubIdx (int curIdx) {
        if (curIdx == 2) {
            return 0;
        } else if (curIdx < 2) {
            return ++curIdx;
        } else
            throw new RuntimeException();
    }
}
