package com.example.movie.controller;

import com.example.movie.model.CinemaBookingRequest;
import com.example.movie.service.BookingPublisherService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class MovieBookingController {

    private final BookingPublisherService publisherService;

    /**
     * POST /bookings - nhan yeu cau dat ve tu client, khoi tao luong Saga.
     */
    @PostMapping("/bookings")
    public ResponseEntity<String> createBooking(@RequestBody CinemaBookingRequest request) {
        publisherService.createBooking(request);
        return ResponseEntity.accepted()
                .body("Booking " + request.getCinemaBookingId() + " da duoc tiep nhan.");
    }
}
