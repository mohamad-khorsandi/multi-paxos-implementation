package org.example;

import java.util.ArrayList;
import java.util.List;


class ProposerAgent{
    public ProposerAgent(ArrayList<AcceptorServiceGrpc.AcceptorServiceBlockingStub> acceptorStubs, int initialProposalNumber) {
        this.nextProposalNumber = initialProposalNumber;
        this.stubs = acceptorStubs;
    }

    private int nextProposalNumber;
    private final List<AcceptorServiceGrpc.AcceptorServiceBlockingStub> stubs;


    public boolean propose(String proposeValue, int sequenceNumber) {
        int proposalNumber = nextProposalNumber;
        nextProposalNumber = nextProposalNumber + 3;
        boolean acceptanceStatus = false;

        PreparePhaseOutcome preparePhaseOutcome = PreparePhase(proposalNumber, sequenceNumber);

        if (! preparePhaseOutcome.majorityPrepare)
            System.out.println("PROPOSER_AGENT: majority DIDN'T accepted me in prepare phase");
        else {
            System.out.println("PROPOSER_AGENT: majority accepted me in prepare phase for sequence number " + sequenceNumber);

            boolean alreadyAccepted = false;
            if (preparePhaseOutcome.maxReceivedProposalNumber > 0) {
                System.out.println("PROPOSER_AGENT: but seems like consensus has already been reached on \"" + proposeValue + "\" so let's stick with that.");
                proposeValue = preparePhaseOutcome.valueOfMaxReceivedProposalNumber;
                alreadyAccepted = true;
            }

            boolean majorityAcceptance = AcceptPhase(proposeValue, proposalNumber, sequenceNumber);
            if (! alreadyAccepted && majorityAcceptance)
                acceptanceStatus = true;
        }

        return acceptanceStatus;
    }


    PreparePhaseOutcome PreparePhase(int proposalNumber, int sequenceNumber) {
        PromiseRequest promiseRequest = PromiseRequest.newBuilder()
                .setProposalNumber(proposalNumber)
                .setSequenceNumber(sequenceNumber)
                .build();

        int acceptedCount = 0;
        int maxReceivedProposalNumber = 0;
        String valueOfMaxReceivedProposalNumber = "";
        System.out.println("PROPOSER_AGENT: Im proposing with proposal number " + proposalNumber + " for sequence number " + sequenceNumber);

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
            } catch (Exception e) {
                System.out.println("PROPOSER_AGENT: " + stub.getChannel().authority() + " is down, could not send PROMISE (" + e.getMessage() + ")");
            }
        }
        return new PreparePhaseOutcome(acceptedCount > 1, maxReceivedProposalNumber, valueOfMaxReceivedProposalNumber);
    }


    boolean AcceptPhase(String proposeValue, int proposeNumber, int sequenceNumber) {
        AcceptRequest acceptRequest = AcceptRequest.newBuilder()
                .setProposalNumber(proposeNumber)
                .setProposalValue(proposeValue)
                .setSequenceNumber(sequenceNumber).build();

        System.out.println("PROPOSER_AGENT: Im requesting others to accept \"" + proposeValue + "\" with proposal number of " + proposeNumber + " for sequence number of " + sequenceNumber);
        int acceptedCount = 0;
        for (AcceptorServiceGrpc.AcceptorServiceBlockingStub stub : stubs) {
            try {
                AcceptResponse acceptResponse = stub.accept(acceptRequest);
                if (acceptResponse.getStatus().equals(AcceptResponse.Status.OK)) acceptedCount++;

            } catch (Exception e) {
                System.out.println("PROPOSER_AGENT: " + stub.getChannel().authority() + " seems to be down(could not send ACCEPT)");
            }
        }

        return acceptedCount > 1;
    }

}


class PreparePhaseOutcome {
    public PreparePhaseOutcome(boolean majorityPrepare, int maxReceivedProposalNumber, String valueOfMaxReceivedProposalNumber) {
        this.majorityPrepare = majorityPrepare;
        this.maxReceivedProposalNumber = maxReceivedProposalNumber;
        this.valueOfMaxReceivedProposalNumber = valueOfMaxReceivedProposalNumber;
    }

    boolean majorityPrepare;
    int maxReceivedProposalNumber;
    String valueOfMaxReceivedProposalNumber;
}

