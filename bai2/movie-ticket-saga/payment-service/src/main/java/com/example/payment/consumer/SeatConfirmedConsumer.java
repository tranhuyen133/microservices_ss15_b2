package com.example.payment.consumer;

import com.example.payment.model.PaymentEvent;
import com.example.payment.service.PaymentProcessingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Consume seat-confirmed-events, trich correlationId tu HEADER, thanh toan.
 * Day la diem cuoi cua luong - chi consume, khong publish tiep.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class SeatConfirmedConsumer {

    public static final String CORRELATION_ID = "correlationId";

    private final PaymentProcessingService paymentService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "seat-confirmed-events", groupId = "payment-group")
    public void handleSeatConfirmed(ConsumerRecord<String, String> record) {
        String correlationId = extractCorrelationId(record);
        MDC.put(CORRELATION_ID, correlationId);

        try {
            log.info("[PaymentService] Processing Payment for {}. CorrelationID: {}",
                    record.key(), correlationId);

            PaymentEvent event = objectMapper.readValue(record.value(), PaymentEvent.class);
            boolean success = paymentService.processPayment(event);

            if (success) {
                log.info("[PaymentService] Payment success: {} VND. CorrelationID: {}",
                        event.getTotalPrice(), correlationId);
            } else {
                log.warn("[PaymentService] Payment failed for {}. CorrelationID: {}",
                        record.key(), correlationId);
            }

        } catch (Exception e) {
            log.error("Error processing payment: {}", e.getMessage(), e);
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
