package com.example.movie.service;

import com.example.movie.model.CinemaBookingRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Publisher khoi tao luong Saga.
 * Sinh correlationId DUY NHAT mot lan roi gan vao header tin nhan.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class BookingPublisherService {

    public static final String CORRELATION_ID = "correlationId";
    public static final String TOPIC = "booking-events";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void createBooking(CinemaBookingRequest request) {
        // BUOC 1: sinh correlationId ngau nhien tai diem khoi dau luong
        String correlationId = UUID.randomUUID().toString();
        MDC.put(CORRELATION_ID, correlationId); // dua vao MDC de moi log tu dong kem ID

        try {
            log.info("[MovieBookingService] Created booking {}. CorrelationID: {}",
                    request.getCinemaBookingId(), correlationId);

            String payload = objectMapper.writeValueAsString(request);

            ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC,
                    request.getCinemaBookingId(), // key = bookingId
                    payload
            );

            // BUOC 2: gan correlationId vao HEADER - KHONG dat trong payload
            record.headers().add(CORRELATION_ID,
                    correlationId.getBytes(StandardCharsets.UTF_8));

            kafkaTemplate.send(record);
            log.info("[MovieBookingService] Published BookingCreated to '{}'. CorrelationID: {}",
                    TOPIC, correlationId);

        } catch (JsonProcessingException e) {
            log.error("Error serializing booking request", e);
        } finally {
            MDC.remove(CORRELATION_ID);
        }
    }
}
