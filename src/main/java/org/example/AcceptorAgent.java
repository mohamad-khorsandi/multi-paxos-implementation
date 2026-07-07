package org.example;


import io.grpc.stub.StreamObserver;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

class AcceptorAgent extends AcceptorServiceGrpc.AcceptorServiceImplBase {
    public AcceptorAgent(List<LearnerServiceGrpc.LearnerServiceBlockingStub> LearnerStubs, int selfPortNumber) {
        STATE_FILE = "replica" + selfPortNumber + ".properties";
        loadState();
        this.stubs = LearnerStubs;
    }
    private final String STATE_FILE;
    private final List<LearnerServiceGrpc.LearnerServiceBlockingStub> stubs;

    int promisedProposalNumber = 0;

    int acceptedProposalNumber = 0;
    String acceptedValue = "";

    @Override
    public void promise(PromiseRequest request, StreamObserver<PromiseResponse> responseObserver) {
        PromiseResponse.Status status;
        if (request.getProposalNumber() > promisedProposalNumber) {
            promisedProposalNumber = request.getProposalNumber();
            saveState();
            status = PromiseResponse.Status.OK;
            System.out.println("ACCEPTOR_AGENT: I made a new promise: " + promisedProposalNumber);
        } else {
            status = PromiseResponse.Status.REJECT;
            System.out.println("ACCEPTOR_AGENT: I rejected a promise with number: " + request.getProposalNumber() + ". I have promised to ignore anything under " + promisedProposalNumber);
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
            saveState();
            status = AcceptResponse.Status.OK;
            System.out.println("ACCEPTOR_AGENT: I accepted this value: " + acceptedValue);

            InternalLearnRequest internalLearnRequest = InternalLearnRequest.newBuilder()
                    .setAcceptedValue(acceptedValue)
                    .setAcceptedProposalNumber(acceptedProposalNumber)
                    .build();
            for (LearnerServiceGrpc.LearnerServiceBlockingStub stub : stubs) {
                try {
                    stub.internalLearn(internalLearnRequest);
                } catch (Exception e) {
                    System.out.println("ACCEPTOR_AGENT: some peer seems to be down(could not send DECIDE)");
                }
            }

        } else {
            status = AcceptResponse.Status.REJECT;
            System.out.println("ACCEPTOR_AGENT: I rejected an accept with number: " + request.getProposalNumber() + ". I have promised to ignore anything under " + this.promisedProposalNumber);
        }

        AcceptResponse response = AcceptResponse.newBuilder()
                .setStatus(status)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    @Override
    public void inform(InformRequest request, StreamObserver<InformResponse> responseObserver) {
        InformResponse.Status status = InformResponse.Status.INCOMPLETE;
        if (acceptedProposalNumber > 0) status = InformResponse.Status.DONE;

        InformResponse informResponse = InformResponse.newBuilder()
                .setStatus(status)
                .setAcceptedProposalNumber(acceptedProposalNumber)
                .setAcceptedValue(acceptedValue).build();

        responseObserver.onNext(informResponse);
        responseObserver.onCompleted();
    }

    Properties props = new Properties();

    public void saveState() {
        try (FileOutputStream out = new FileOutputStream(STATE_FILE)) {
            props.setProperty("promisedProposalNumber", String.valueOf(this.promisedProposalNumber));
            props.setProperty("acceptedProposalNumber", String.valueOf(this.acceptedProposalNumber));
            props.setProperty("acceptedValue", this.acceptedValue);
            props.store(out, "Acceptor State");
        } catch (IOException e) {
            System.err.println("Error saving state: " + e.getMessage());
        }
    }

    public void loadState() {
        try (FileInputStream in = new FileInputStream(STATE_FILE)) {
            props.load(in);
            this.promisedProposalNumber = Integer.parseInt(props.getProperty("promisedProposalNumber", "0"));
            this.acceptedProposalNumber = Integer.parseInt(props.getProperty("acceptedProposalNumber", "0"));
            this.acceptedValue = props.getProperty("acceptedValue", "");
        } catch (IOException ignored) {

        }
    }
}