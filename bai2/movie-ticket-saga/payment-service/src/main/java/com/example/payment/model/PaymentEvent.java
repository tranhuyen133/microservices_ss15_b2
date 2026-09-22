package com.example.payment.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Su kien tuong ung voi SeatConfirmed nhan tu SeatAllocationService.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {
    private String cinemaBookingId;
    private String movieCode;
    private List<String> reservedSeats;
    private String customerEmail;
    private long totalPrice;
}
