package org.example;


import io.grpc.stub.StreamObserver;

class AcceptorAgent extends AcceptorServiceGrpc.AcceptorServiceImplBase {
    int promisedProposalNumber = 0;

    int acceptedProposalNumber = 0;
    String acceptedValue = "";

    @Override
    public void promise(PromiseRequest request, StreamObserver<PromiseResponse> responseObserver) {
        PromiseResponse.Status status;
        if (request.getProposalNumber() > promisedProposalNumber) {
            promisedProposalNumber = request.getProposalNumber();
            status = PromiseResponse.Status.OK;
            System.out.println("ACCEPTOR_AGENT: I made a new promise: " + promisedProposalNumber);
        } else {
            status = PromiseResponse.Status.REJECT;
        }

        PromiseResponse response = PromiseResponse.newBuilder()
                .setStatus(status)
                .setAcceptedProposalNumber(acceptedProposalNumber)
                .setAcceptedValue(acceptedValue)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void accept(AcceptRequest request, StreamObserver<AcceptResponse> responseObserver) {
        AcceptResponse.Status status;
        if (request.getProposalNumber() >= promisedProposalNumber) {
            acceptedProposalNumber = request.getProposalNumber();
            acceptedValue = request.getProposalValue();
            status = AcceptResponse.Status.OK;
            System.out.println("ACCEPTOR_AGENT: I accepted this value: " + acceptedValue);
        } else {
            status = AcceptResponse.Status.REJECT;
        }

        AcceptResponse response = AcceptResponse.newBuilder()
                .setStatus(status)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}