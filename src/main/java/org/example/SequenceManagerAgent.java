package org.example;

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;

public class SequenceManagerAgent extends SequenceManagerServiceGrpc.SequenceManagerServiceImplBase {
    @Override
    public void addCommand(AddCmdRequest request, StreamObserver<AddCmdResponse> responseObserver) {
        System.out.println("================================ADD COMMAND================================");
        List<ChosenEntry> learnedSequence = ServerMain.learnerAgent.learn();
        int nextSeqNum = getNextSeqNum(learnedSequence);
        System.out.println("SEQUENCE_MANAGER: I've chosen " +nextSeqNum+ " as sequence number.");
        boolean result = ServerMain.proposerAgent.propose(request.getValue(), nextSeqNum);
        AddCmdResponse.Status status;
        if (result) status = AddCmdResponse.Status.SUCCESS;
        else status = AddCmdResponse.Status.FAIL;

        responseObserver.onNext(AddCmdResponse.newBuilder().setStatus(status).setCommandNumber(nextSeqNum).build());
        responseObserver.onCompleted();
    }


    @Override
    public void listCommand(ListCmdRequest request, StreamObserver<ListCmdResponse> responseObserver) {
        System.out.println("================================LIST COMMAND================================");
        List<ChosenEntry> acceptedValues = ServerMain.learnerAgent.learn();

        ArrayList<String> result = new ArrayList<>();
        int i = 0;
        for (ChosenEntry pair : acceptedValues) {
            while (pair.seqNum != i) {
                result.add("-");
                i++;
            }
            result.add(pair.value);
            i++;
        }

        responseObserver.onNext(ListCmdResponse.newBuilder().addAllCommands(result).build());
        responseObserver.onCompleted();
    }

    private static int getNextSeqNum (List<ChosenEntry> learnedSequence) {
        int nextSeqNum = 0;
        if (!learnedSequence.isEmpty()) {
            int max = learnedSequence.stream().map(ChosenEntry::getSeqNum).max(Integer::compare).orElseThrow();

            if (learnedSequence.size() < max+1) {
                for (int i = 0; i < learnedSequence.size(); i++)
                    if (learnedSequence.get(i).seqNum != i) {
                        nextSeqNum = i;
                        break;
                    }
            }
            else if (learnedSequence.size() == max+1)
                nextSeqNum = learnedSequence.size();

            else
                System.out.println("SEQUENCE_MANAGER: Warning about distinction of sequence numbers");
        }
        return nextSeqNum;
    }
}


class ChosenEntry {
    ChosenEntry(int seqNum, String value) {
        this.seqNum = seqNum;
        this.value = value;
    }
    final int seqNum;
    final String value;

    public int getSeqNum() {
        return seqNum;
    }
}