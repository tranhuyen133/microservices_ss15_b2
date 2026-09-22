package com.example.movie.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Yeu cau dat ve tu client. LUU Y: khong chua correlationId trong payload -
 * correlationId duoc gan vao HEADER cua tin nhan Kafka.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CinemaBookingRequest {
    private String cinemaBookingId;
    private String movieCode;
    private String showTime;
    private List<String> seatNumbers;
    private String customerEmail;
    private long totalPrice;
}
