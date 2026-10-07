package org.example;


import io.grpc.stub.StreamObserver;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

class AcceptorAgent extends AcceptorServiceGrpc.AcceptorServiceImplBase {
    public AcceptorAgent(List<LearnerServiceGrpc.LearnerServiceBlockingStub> LearnerStubs, int replicaNumber) {
        this.stubs = LearnerStubs;
        this.replicaNumber = replicaNumber;
    }
    private final List<LearnerServiceGrpc.LearnerServiceBlockingStub> stubs;
    private final int replicaNumber;


    @Override
    public void promise(PromiseRequest request, StreamObserver<PromiseResponse> responseObserver) {
        AcceptorState state = new AcceptorState(this.replicaNumber, request.getSequenceNumber());

        PromiseResponse.Status status;
        if (request.getProposalNumber() > state.promisedProposalNumber) {
            state.promisedProposalNumber = request.getProposalNumber();
            state.saveState();
            status = PromiseResponse.Status.OK;
            System.out.println("ACCEPTOR_AGENT: I made a new promise (" + state.promisedProposalNumber + ") for sequence number " + request.getSequenceNumber());
        } else {
            status = PromiseResponse.Status.REJECT;
            System.out.println("ACCEPTOR_AGENT: I rejected a promise with number: " + request.getProposalNumber() + ". I have promised to ignore anything under " + state.promisedProposalNumber + " for sequence number " + request.getSequenceNumber());
        }

        PromiseResponse response = PromiseResponse.newBuilder()
                .setStatus(status)
                .setAcceptedProposalNumber(state.acceptedProposalNumber)
                .setAcceptedValue(state.acceptedValue)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    @Override
    public void accept(AcceptRequest request, StreamObserver<AcceptResponse> responseObserver) {
        AcceptorState state = new AcceptorState(this.replicaNumber, request.getSequenceNumber());
        AcceptResponse.Status status;
        if (request.getProposalNumber() >= state.promisedProposalNumber) {
            state.acceptedProposalNumber = request.getProposalNumber();
            state.acceptedValue = request.getProposalValue();
            state.saveState();
            status = AcceptResponse.Status.OK;
            System.out.println("ACCEPTOR_AGENT: I accepted this value: " + state.acceptedValue + " for this sequence number " + request.getSequenceNumber());

            InternalLearnRequest internalLearnRequest = InternalLearnRequest.newBuilder()
                    .setSequenceNumber(request.getSequenceNumber())
                    .setAcceptedValue(state.acceptedValue)
                    .setReplicaNumber(this.replicaNumber)
                    .build();
            for (LearnerServiceGrpc.LearnerServiceBlockingStub stub : stubs) {
                try {
                    stub.internalLearn(internalLearnRequest);
                } catch (Exception e) {
                    System.out.println("ACCEPTOR_AGENT: "+ stub.getChannel().authority() +" seems to be down(could not send DECIDE for sequence number " + request.getSequenceNumber() + ")");
                }
            }

        } else {
            status = AcceptResponse.Status.REJECT;
            System.out.println("ACCEPTOR_AGENT: I rejected an accept with number: " + request.getProposalNumber() + ". I have promised to ignore anything under " + state.promisedProposalNumber + " for sequence number: " + request.getSequenceNumber());
        }

        responseObserver.onNext(AcceptResponse.newBuilder().setStatus(status).build());
        responseObserver.onCompleted();
    }


    @Override
    public void inform(InformRequest request, StreamObserver<InformResponse> responseObserver) {
        InformResponse.Builder informResponse = InformResponse.newBuilder();

        for (String propFile : getAllPropertyFiles()) {
            int sequenceNumber = Integer.parseInt(propFile.split("[.\\-]")[1]);
            AcceptorState state = new AcceptorState(propFile);
            if (state.acceptedProposalNumber == 0) continue;

            informResponse.addAcceptedValues(
                    InformResponse.Pair.newBuilder().setSequenceNumber(sequenceNumber).setAcceptedValue(state.acceptedValue)
            );
        }
        responseObserver.onNext(informResponse.build());
        responseObserver.onCompleted();
    }


    private List<String> getAllPropertyFiles() {
        File currentDir = new File(".");

        FilenameFilter filter = (dir, name) ->
                name.startsWith(this.replicaNumber + "-") && name.endsWith(".properties");

        File[] files = currentDir.listFiles(filter);

        if (files == null || files.length == 0) {
            return List.of();
        }

        return Arrays.stream(files)
                .map(File::getName)
                .collect(Collectors.toList());
    }

}


class AcceptorState {
    AcceptorState(int replicaNumber, int sequenceNumber) {
        STATE_FILE = replicaNumber + "-" + sequenceNumber + ".properties";
        if (new File(STATE_FILE).exists())
            this.loadState();
    }

    AcceptorState(String stateFile) {
        STATE_FILE = stateFile;
        if (new File(STATE_FILE).exists())
            this.loadState();
    }

    private final String STATE_FILE;
    int promisedProposalNumber = 0;
    int acceptedProposalNumber = 0;
    String acceptedValue = "";

    private void loadState() {


        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(STATE_FILE)) {
            props.load(in);
            this.promisedProposalNumber = Integer.parseInt(props.getProperty("promisedProposalNumber", "0"));
            this.acceptedProposalNumber = Integer.parseInt(props.getProperty("acceptedProposalNumber", "0"));
            this.acceptedValue = props.getProperty("acceptedValue", "");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public void saveState() {
        Properties props = new Properties();
        try (FileOutputStream out = new FileOutputStream(STATE_FILE)) {
            props.setProperty("promisedProposalNumber", String.valueOf(this.promisedProposalNumber));
            props.setProperty("acceptedProposalNumber", String.valueOf(this.acceptedProposalNumber));
            props.setProperty("acceptedValue", this.acceptedValue);
            props.store(out, "Acceptor State");
        } catch (IOException e) {
            System.err.println("Error saving state: " + e.getMessage());
        }
    }
}

