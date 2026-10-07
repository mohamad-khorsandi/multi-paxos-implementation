package org.example;

import io.grpc.Deadline;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;


class LearnerAgent extends LearnerServiceGrpc.LearnerServiceImplBase {
    LearnerAgent(List<AcceptorServiceGrpc.AcceptorServiceBlockingStub> acceptorStubs) {
        this.stubs = acceptorStubs;
    }

    private final List<AcceptorServiceGrpc.AcceptorServiceBlockingStub> stubs;

    public List<ChosenEntry> learn() {
        System.out.println("LEARNER AGENT: I have been asked to learn the whole sequence.");
        HashMap<Integer, Set<String>> acceptedValues = new HashMap<>();
        final HashMap<Integer, String> chosenValues = new HashMap<>();
        InformRequest informRequest = InformRequest.newBuilder().build();

        for (AcceptorServiceGrpc.AcceptorServiceBlockingStub stub : stubs) {
            try {
                InformResponse informResponse = stub.inform(informRequest);
                for (InformResponse.Pair pair : informResponse.getAcceptedValuesList()) {
                    if (acceptedValues.containsKey(pair.getSequenceNumber())) {
                        boolean distinct = acceptedValues.get(pair.getSequenceNumber()).add(pair.getAcceptedValue());
                        if (!distinct)
                            chosenValues.put(pair.getSequenceNumber(), pair.getAcceptedValue());
                    } else {
                        HashSet<String> newHashSet = new HashSet<>();
                        newHashSet.add(pair.getAcceptedValue());
                        acceptedValues.put(pair.getSequenceNumber(), newHashSet);
                    }
                }
            } catch (Exception e) {
                System.out.println("LEARNER AGENT: Seems like " + stub.getChannel().authority() + " is down. (" + e.getMessage() + ")");
            }
        }

        System.out.println("LEARNER AGENT: Currently there are " + chosenValues.size() + " chosen commands.");

        return chosenValues.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new ChosenEntry(entry.getKey(), entry.getValue()))
                .toList();
    }


    HashMap<Integer, HashMap<Integer, String>> acceptedValues = new HashMap<>();
    HashMap<Integer, String> chosenValues = new HashMap<>();

    @Override
    public void internalLearn(InternalLearnRequest request, StreamObserver<InternalLearnResponse> responseObserver) {
        if (acceptedValues.containsKey(request.getSequenceNumber()))
            acceptedValues.get(request.getSequenceNumber()).put(request.getReplicaNumber(), request.getAcceptedValue());
        else {
            HashMap<Integer, String> hashMap = new HashMap<>();
            hashMap.put(request.getReplicaNumber(), request.getAcceptedValue());
            acceptedValues.put(request.getSequenceNumber(), hashMap);
        }
        Collection<String> acceptedValuesForSeqNum = acceptedValues.get(request.getSequenceNumber()).values();
        HashSet<String> hashSet = new HashSet<>();

        String chosenValue;
        for (String value : acceptedValuesForSeqNum) {
            if(!hashSet.add(value)) {
                chosenValue = value;
                System.out.println("LEARNER AGENT: I've learned from acceptor requests that the value \"" + chosenValue + "\" is chosen for sequence number " + request.getSequenceNumber());
                chosenValues.put(request.getSequenceNumber(), chosenValue);
                break;
            }
        }

        responseObserver.onNext(InternalLearnResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}