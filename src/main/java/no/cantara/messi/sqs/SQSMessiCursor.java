package no.cantara.messi.sqs;

import de.huxhorn.sulky.ulid.ULID;
import no.cantara.messi.api.MessiCursor;
import no.cantara.messi.api.MessiCursorStartingPointType;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

class SQSMessiCursor implements MessiCursor {

    MessiCursorStartingPointType type;

    SQSMessiCursor(MessiCursorStartingPointType type) {
        this.type = type;
    }

    @Override
    public String checkpoint() {
        throw new UnsupportedOperationException();
    }

    static class Builder implements MessiCursor.Builder {

        MessiCursorStartingPointType type;

        @Override
        public Builder shardId(String shardId) {
            return this;
        }

        @Override
        public Builder now() {
            this.type = MessiCursorStartingPointType.NOW;
            return this;
        }

        @Override
        public Builder oldest() {
            this.type = MessiCursorStartingPointType.OLDEST_RETAINED;
            return this;
        }

        @Override
        public Builder providerTimestamp(Instant timestamp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Builder providerSequenceNumber(String sequenceNumber) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Builder ulid(ULID.Value ulid) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Builder externalId(String externalId, Instant externalIdTimestamp, Duration externalIdTimestampTolerance) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Builder inclusive(boolean inclusive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessiCursor.Builder checkpoint(String checkpoint) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SQSMessiCursor build() {
            Objects.requireNonNull(type);
            return new SQSMessiCursor(type);
        }
    }
}
