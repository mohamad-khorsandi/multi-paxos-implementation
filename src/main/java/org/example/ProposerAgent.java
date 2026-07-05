package org.example;

import io.grpc.Internal;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.example.AcceptorServiceGrpc.newBlockingStub;


class ProposerAgent extends ProposerServiceGrpc.ProposerServiceImplBase{
    public ProposerAgent(int nextProposalNumber, List<Integer> peerPortNumbers) {
        this.nextProposalNumber = nextProposalNumber;
        stubs = new ArrayList<>();
        for (int port : peerPortNumbers) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build();
            stubs.add(newBlockingStub(channel));
        }
    }

    private int nextProposalNumber;
    final private ArrayList<AcceptorServiceGrpc.AcceptorServiceBlockingStub> stubs;

    @Override
    public void propose(ProposeRequest request, StreamObserver<ProposeResponse> responseObserver) {
        int proposalNumber = nextProposalNumber;
        nextProposalNumber = nextProposalNumber + 3;
        String proposalValue = request.getProposalValue();

        PromiseRequest promiseRequest = PromiseRequest.newBuilder()
                .setProposalNumber(proposalNumber)
                .build();

        int acceptedCount = 0;
        int maxReceivedProposalNumber = 0;
        String valueOfMaxReceivedProposalNumber = "";
        System.out.println("PROPOSER_AGENT: im proposing \"" + proposalValue + "\" with proposal number: " + proposalNumber);
        for (AcceptorServiceGrpc.AcceptorServiceBlockingStub stub : stubs) {

            PromiseResponse res = stub.promise(promiseRequest);
            if (res.getStatus().equals(PromiseResponse.Status.OK)) {
                acceptedCount++;
                if (res.getAcceptedProposalNumber() > 0) {
                    if (res.getAcceptedProposalNumber() > maxReceivedProposalNumber) {
                        maxReceivedProposalNumber = res.getAcceptedProposalNumber();
                        valueOfMaxReceivedProposalNumber = res.getAcceptedValue();
                    }
                }
            }
        }

        ProposeResponse proposeResponse;

        if (acceptedCount > 1) {
            System.out.println("PROPOSER_AGENT: majority accepted my proposal");
            AcceptRequest acceptRequest;
            String finalProposeValue;
            ProposeResponse.Status finalStatus = ProposeResponse.Status.OK;

            if (maxReceivedProposalNumber > 0) {
                finalProposeValue = valueOfMaxReceivedProposalNumber;
                finalStatus = ProposeResponse.Status.REJECT;
                System.out.println("PROPOSER_AGENT: but seems like consensus has already been reached on \"" + valueOfMaxReceivedProposalNumber + "\" so let's stick with that.");
            }
            else {
                finalProposeValue = proposalValue;
            }
            acceptRequest = AcceptRequest.newBuilder()
                    .setProposalNumber(proposalNumber)
                    .setProposalValue(finalProposeValue)
                    .build();

            for (AcceptorServiceGrpc.AcceptorServiceBlockingStub stub : stubs)
                stub.accept(acceptRequest);

            proposeResponse = ProposeResponse.newBuilder().setStatus(finalStatus).setAcceptedValue(finalProposeValue).build();
        } else {
            proposeResponse = ProposeResponse.newBuilder().setStatus(ProposeResponse.Status.REJECT).setAcceptedValue("").build();
        }

        responseObserver.onNext(proposeResponse);
        responseObserver.onCompleted();
    }
}
