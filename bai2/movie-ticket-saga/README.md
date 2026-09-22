# Movie Ticket Saga — Correlation ID & Tracing

Bài tập 2: 3 microservice giao tiếp qua Kafka theo Choreography Saga, dùng Correlation ID (trong header Kafka) để trace toàn bộ hành trình đặt vé.

## Nội dung nộp bài

| File | Mô tả |
|---|---|
| `BAO-CAO.md` | Báo cáo: luồng sự kiện, kỹ thuật header vs payload, hướng dẫn chạy, kết quả |
| `movie-booking-service/` | Producer khởi tạo — sinh correlationId, publish `booking-events` |
| `seat-allocation-service/` | Consumer + Producer — giữ ghế, truyền tiếp correlationId sang `seat-confirmed-events` |
| `payment-service/` | Consumer cuối — thanh toán |
| `docker-compose.yml` | Kafka + Zookeeper |

## Cấu trúc

```
movie-ticket-saga/
├── docker-compose.yml
├── BAO-CAO.md
├── movie-booking-service/     (port 8081)  → topic: booking-events
├── seat-allocation-service/   (port 8082)  → topic: seat-confirmed-events
└── payment-service/           (port 8083)
```

## Chạy nhanh

```bash
# 1. Kafka
docker compose up -d

# 2. Ba service (mỗi terminal một lệnh)
cd movie-booking-service   && mvn spring-boot:run
cd seat-allocation-service && mvn spring-boot:run
cd payment-service         && mvn spring-boot:run

# 3. Gửi request
curl -X POST http://localhost:8081/bookings \
  -H "Content-Type: application/json" \
  -d '{"cinemaBookingId":"CIN-2024-789","movieCode":"AVENGERS-5","showTime":"2024-12-25T19:30:00","seatNumbers":["A12","A13"],"customerEmail":"tuananh@email.com","totalPrice":240000}'
```

## Điểm kỹ thuật chính

- **correlationId sinh 1 lần** ở `BookingPublisherService`, gắn vào **header** (không phải payload).
- Consumer trích qua `record.headers().lastHeader("correlationId")`, **null-safe**.
- `SeatAllocationService` **truyền tiếp** đúng correlationId sang event kế tiếp.
- **MDC** + log pattern `[%X{correlationId}]` → mọi dòng log tự động kèm ID.

## Đẩy lên GitHub

```bash
cd movie-ticket-saga
git init && git add . && git commit -m "Correlation ID tracing qua Kafka - Choreography Saga"
git branch -M main
git remote add origin https://github.com/<username>/movie-ticket-saga.git
git push -u origin main
```
