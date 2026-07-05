package org.example;

import io.grpc.stub.StreamObserver;


class LearnerAgent extends LearnerServiceGrpc.LearnerServiceImplBase {
    @Override
    public void learn(ExternalLearnRequest request, StreamObserver<LearnResponse> responseObserver) {
        super.learn(request, responseObserver);
    }

    @Override
    public void internalLearn(InternalLearnRequest request, StreamObserver<LearnResponse> responseObserver) {
        super.internalLearn(request, responseObserver);
    }
}