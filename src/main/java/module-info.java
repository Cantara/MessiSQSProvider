module messi.provider.sqs {
    requires messi.sdk;
    requires property.config;

    requires org.slf4j;
    requires de.huxhorn.sulky.ulid;
    requires com.google.protobuf;
    requires com.google.protobuf.util;

    requires software.amazon.awssdk.auth;
    requires software.amazon.awssdk.awscore;
    requires software.amazon.awssdk.core;
    requires software.amazon.awssdk.http;
    requires software.amazon.awssdk.regions;
    requires software.amazon.awssdk.services.sqs;

    provides no.cantara.messi.api.MessiClientFactory with
            no.cantara.messi.sqs.SQSMessiClientFactory,
            no.cantara.messi.sqs.simulator.SQSSimulatorMessiClientFactory;

    exports no.cantara.messi.sqs;
    exports no.cantara.messi.sqs.simulator;
}
