package com.example.seat.consumer;

import com.example.seat.model.SeatAllocationEvent;
import com.example.seat.producer.SeatConfirmedProducer;
import com.example.seat.service.SeatAllocationService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Consume booking-events, trich correlationId tu HEADER (khong lay tu payload),
 * giu ghe, roi publish SeatConfirmed kem correlationId.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class BookingEventConsumer {

    public static final String CORRELATION_ID = "correlationId";

    private final SeatAllocationService seatService;
    private final SeatConfirmedProducer seatConfirmedProducer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "booking-events", groupId = "seat-group")
    public void handleBooking(ConsumerRecord<String, String> record) {
        // Trich correlationId tu HEADER (null-safe: fallback neu thieu)
        String correlationId = extractCorrelationId(record);
        MDC.put(CORRELATION_ID, correlationId);

        try {
            log.info("[SeatAllocationService] Received SeatRequest for {}. CorrelationID: {}",
                    record.key(), correlationId);

            JsonNode json = objectMapper.readTree(record.value());
            List<String> seatNumbers = new ArrayList<>();
            json.get("seatNumbers").forEach(n -> seatNumbers.add(n.asText()));

            String seatResult = seatService.reserveSeats(seatNumbers);
            log.info("[SeatAllocationService] Seat reserved: {}. CorrelationID: {}",
                    seatResult, correlationId);

            SeatAllocationEvent event = new SeatAllocationEvent(
                    json.get("cinemaBookingId").asText(),
                    json.get("movieCode").asText(),
                    seatNumbers,
                    json.get("customerEmail").asText(),
                    json.get("totalPrice").asLong()
            );
            seatConfirmedProducer.publishSeatConfirmed(event, correlationId);

        } catch (Exception e) {
            log.error("Error processing booking: {}", e.getMessage(), e);
        } finally {
            MDC.remove(CORRELATION_ID);
        }
    }

    private String extractCorrelationId(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(CORRELATION_ID);
        if (header != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        String generated = UUID.randomUUID().toString();
        log.warn("Thieu correlationId trong header, tao moi: {}", generated);
        return generated;
    }
}
