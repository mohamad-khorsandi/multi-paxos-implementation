package org.example;

import io.grpc.Internal;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.example.AcceptorServiceGrpc.newBlockingStub;


class ProposerAgent extends ProposerServiceGrpc.ProposerServiceImplBase{
    public ProposerAgent(int initialProposalNumber, List<AcceptorServiceGrpc.AcceptorServiceBlockingStub> acceptorStubs) {
        this.nextProposalNumber = initialProposalNumber;
        this.stubs = acceptorStubs;
    }

    private int nextProposalNumber;
    private final List<AcceptorServiceGrpc.AcceptorServiceBlockingStub> stubs;

    @Override
    public void propose(ProposeRequest request, StreamObserver<ProposeResponse> responseObserver) {
        int proposalNumber = nextProposalNumber;
        nextProposalNumber = nextProposalNumber + 3;
        String proposeValue = request.getProposalValue();

        PromiseRequest promiseRequest = PromiseRequest.newBuilder()
                .setProposalNumber(proposalNumber)
                .build();

        int acceptedCount = 0;
        int maxReceivedProposalNumber = 0;
        String valueOfMaxReceivedProposalNumber = "";
        System.out.println("PROPOSER_AGENT: im proposing with proposal number: " + proposalNumber);
        for (AcceptorServiceGrpc.AcceptorServiceBlockingStub stub : stubs) {

            try {
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
            } catch (Exception io) {
                System.out.println("PROPOSER_AGENT: some peer seems to be down(could not send PROMISE)");
            }

        }

        ProposeResponse.Status consensusStatus = ProposeResponse.Status.FAIL;
        if (acceptedCount > 1) {
            System.out.println("PROPOSER_AGENT: majority accepted my proposal");

            if (maxReceivedProposalNumber == 0) {
                consensusStatus = ProposeResponse.Status.SUCCESS;
            } else {
                proposeValue = valueOfMaxReceivedProposalNumber;
                System.out.println("PROPOSER_AGENT: but seems like consensus has already been reached on \"" + proposeValue + "\" so let's stick with that.");
            }

            AcceptRequest acceptRequest = AcceptRequest.newBuilder()
                    .setProposalNumber(proposalNumber)
                    .setProposalValue(proposeValue).build();

            System.out.println("PROPOSER_AGENT: Im requesting others to accept \"" + proposeValue + "\" with proposal number of: " + proposalNumber);
            for (AcceptorServiceGrpc.AcceptorServiceBlockingStub stub : stubs) {
                try {
                    stub.accept(acceptRequest);
                } catch (Exception e) {
                    System.out.println("PROPOSER_AGENT: some peer seems to be down(could not send ACCEPT)");
                }

            }
        } else
            System.out.println("PROPOSER_AGENT: majority DIDN'T accepted my proposal");


        responseObserver.onNext(ProposeResponse.newBuilder().setStatus(consensusStatus).build());
        responseObserver.onCompleted();
    }

}


