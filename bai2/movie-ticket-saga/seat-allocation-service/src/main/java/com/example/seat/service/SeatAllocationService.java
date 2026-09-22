package com.example.seat.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Nghiep vu giu ghe (mo phong).
 */
@Service
@Slf4j
public class SeatAllocationService {

    public String reserveSeats(List<String> seatNumbers) {
        // Mo phong giu ghe thanh cong
        return String.join(", ", seatNumbers);
    }
}
