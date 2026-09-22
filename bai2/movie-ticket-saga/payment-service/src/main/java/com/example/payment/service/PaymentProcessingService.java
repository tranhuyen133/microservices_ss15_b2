package com.example.payment.service;

import com.example.payment.model.PaymentEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Nghiep vu thanh toan (mo phong).
 */
@Service
@Slf4j
public class PaymentProcessingService {

    public boolean processPayment(PaymentEvent event) {
        // Mo phong thanh toan thanh cong
        return event.getTotalPrice() > 0;
    }
}
