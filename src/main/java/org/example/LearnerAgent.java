package org.example;

import io.grpc.stub.StreamObserver;

import java.util.List;


class LearnerAgent extends LearnerServiceGrpc.LearnerServiceImplBase {
    LearnerAgent(List<AcceptorServiceGrpc.AcceptorServiceBlockingStub> acceptorStubs) {
        this.stubs = acceptorStubs;
    }

    private final List<AcceptorServiceGrpc.AcceptorServiceBlockingStub> stubs;
    private int acceptedProposalNumber = 0;
    private String acceptedValue = "";

    @Override
    public void learn(ExternalLearnRequest request, StreamObserver<LearnResponse> responseObserver) {
        System.out.println("LEARNER AGENT: I have been asked to learn weather values is chosen.");

        LearnResponse.Status status = LearnResponse.Status.DONT_KNOW;
        for (AcceptorServiceGrpc.AcceptorServiceBlockingStub stub : stubs) {
            InformResponse informResponse = stub.inform(InformRequest.newBuilder().build());

            if (informResponse.getStatus().equals(InformResponse.Status.DONE)) {
                acceptedProposalNumber = informResponse.getAcceptedProposalNumber();
                acceptedValue = informResponse.getAcceptedValue();
                status = LearnResponse.Status.CHOSEN;
                System.out.println("LEARNER AGENT: I learned that the value \"" +acceptedValue+ "\" is chosen with proposal number of: " + acceptedProposalNumber);
                break;
            }
        }
        LearnResponse learnResponse = LearnResponse.newBuilder().setStatus(status).setAcceptedValue(acceptedValue).build();
        responseObserver.onNext(learnResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void internalLearn(InternalLearnRequest request, StreamObserver<LearnResponse> responseObserver) {
        this.acceptedValue = request.getAcceptedValue();
        this.acceptedProposalNumber = request.getAcceptedProposalNumber();
        System.out.println("LEARNER AGENT: I have learn from an acceptor request the value \"" +acceptedValue+ "\" with number " + acceptedProposalNumber);
        LearnResponse learnResponse = LearnResponse.newBuilder()
                .setStatus(LearnResponse.Status.CHOSEN)
                .setAcceptedValue(acceptedValue).build();
        responseObserver.onNext(learnResponse);
        responseObserver.onCompleted();
    }
}