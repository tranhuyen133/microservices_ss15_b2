package com.example.seat.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Su kien xac nhan da giu ghe, gui sang PaymentService.
 * correlationId KHONG nam trong payload nay - no di theo header.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeatAllocationEvent {
    private String cinemaBookingId;
    private String movieCode;
    private List<String> reservedSeats;
    private String customerEmail;
    private long totalPrice;
}
