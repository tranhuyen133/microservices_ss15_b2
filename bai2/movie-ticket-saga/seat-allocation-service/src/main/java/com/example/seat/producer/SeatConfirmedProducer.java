package com.example.seat.producer;

import com.example.seat.model.SeatAllocationEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Publish su kien SeatConfirmed sang topic seat-confirmed-events,
 * truyen TIEP correlationId (khong sinh moi) vao header.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class SeatConfirmedProducer {

    public static final String CORRELATION_ID = "correlationId";
    public static final String TOPIC = "seat-confirmed-events";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void publishSeatConfirmed(SeatAllocationEvent event, String correlationId) {
        try {
            String payload = objectMapper.writeValueAsString(event);

            ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC,
                    event.getCinemaBookingId(),
                    payload
            );

            // Truyen tiep correlationId nhan duoc tu upstream vao header
            record.headers().add(CORRELATION_ID,
                    correlationId.getBytes(StandardCharsets.UTF_8));

            kafkaTemplate.send(record);
            log.info("[SeatAllocationService] Published SeatConfirmed to '{}'. CorrelationID: {}",
                    TOPIC, correlationId);

        } catch (Exception e) {
            log.error("Error publishing SeatConfirmed: {}", e.getMessage(), e);
        }
    }
}
