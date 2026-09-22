# Báo cáo Bài tập 2: "Bản đồ dẫn đường" với Correlation ID & Tracing

**Bài toán:** Đặt vé xem phim qua 3 microservice giao tiếp bằng Kafka theo mô hình Choreography Saga, dùng Correlation ID để trace toàn bộ hành trình một vé.

**Dữ liệu đầu vào:**
```json
{
  "cinemaBookingId": "CIN-2024-789",
  "movieCode": "AVENGERS-5",
  "showTime": "2024-12-25T19:30:00",
  "seatNumbers": ["A12", "A13"],
  "customerEmail": "tuananh@email.com",
  "totalPrice": 240000
}
```

---

## 1. Luồng sự kiện và vai trò của Correlation ID

```
Client
  │  POST /bookings
  ▼
MovieBookingService ──(topic: booking-events)──▶ SeatAllocationService ──(topic: seat-confirmed-events)──▶ PaymentService
  sinh correlationId                                trích + giữ ghế + truyền tiếp                            trích + thanh toán
  (UUID, 1 lần)                                     correlationId (header)                                  correlationId (header)
```

**Vai trò của Correlation ID:** trong hệ thống event-driven, mỗi service log độc lập. Không có một ID chung, ta không thể biết dòng log ở PaymentService thuộc về yêu cầu đặt vé nào — hiện tượng "Event Spaghetti". Correlation ID là "sợi chỉ đỏ" gắn vào mọi sự kiện của cùng một giao dịch, cho phép:

- Lọc log theo một `correlationId` để dựng lại toàn bộ hành trình của một vé.
- Tích hợp công cụ tracing (Jaeger/Zipkin) để trực quan hóa timeline giữa các service.
- Gỡ lỗi nhanh khi một bước trong Saga thất bại.

**Nguyên tắc quan trọng:** correlationId chỉ được **sinh một lần** tại MovieBookingService (điểm khởi đầu). Hai service sau **đọc từ header và truyền tiếp**, tuyệt đối không sinh mới — nếu không, hành trình bị "đứt đoạn".

---

## 2. Kỹ thuật gắn Correlation ID vào HEADER (không phải payload)

### Cách làm

Producer gắn vào header của `ProducerRecord`:
```java
record.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));
```

Consumer trích từ header (null-safe):
```java
Header header = record.headers().lastHeader("correlationId");
String correlationId = (header != null)
        ? new String(header.value(), StandardCharsets.UTF_8)
        : UUID.randomUUID().toString(); // fallback + log cảnh báo
```

### Vì sao dùng header thay vì payload

1. **Tách metadata khỏi business data.** correlationId là metadata hạ tầng (tracing), không phải dữ liệu nghiệp vụ. Để trong payload làm bẩn schema của event.
2. **Consumer đọc được mà không cần deserialize payload.** Không phụ thuộc kiểu dữ liệu payload, không phải sửa DTO khi thêm service mới.
3. **Không phá vỡ contract.** Thêm/bớt header không ảnh hưởng consumer đang parse payload theo schema cũ.
4. **Chuẩn tracing.** Các framework (Micrometer Tracing, OpenTelemetry) mặc định propagate context qua header — ăn khớp tự nhiên.

### Điểm nâng cao trong bài này

- **MDC (Mapped Diagnostic Context):** đưa correlationId vào `MDC` để pattern log `[%X{correlationId}]` tự động in ID ở **mọi** dòng log, không phải truyền tay từng câu lệnh. Cấu hình trong `application.yml`:
  ```yaml
  logging:
    pattern:
      console: "%d{HH:mm:ss} %-5level [%X{correlationId}] %logger{20} - %msg%n"
  ```
- **Null-safe extraction:** tránh `NullPointerException` khi message thiếu header.

---

## 3. Hướng dẫn cài đặt và chạy

### Bước 1: Khởi động Kafka
```bash
cd movie-ticket-saga
docker compose up -d
```
Topic `booking-events` và `seat-confirmed-events` được tạo tự động (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`). Nếu muốn tạo tay:
```bash
docker exec -it $(docker ps -qf "name=kafka") \
  kafka-topics --create --topic booking-events --bootstrap-server localhost:9092
docker exec -it $(docker ps -qf "name=kafka") \
  kafka-topics --create --topic seat-confirmed-events --bootstrap-server localhost:9092
```

### Bước 2: Chạy 3 service (mỗi lệnh một terminal)
```bash
cd movie-booking-service   && mvn spring-boot:run   # port 8081
cd seat-allocation-service && mvn spring-boot:run    # port 8082
cd payment-service         && mvn spring-boot:run    # port 8083
```

### Bước 3: Gửi request đặt vé
```bash
curl -X POST http://localhost:8081/bookings \
  -H "Content-Type: application/json" \
  -d '{
    "cinemaBookingId": "CIN-2024-789",
    "movieCode": "AVENGERS-5",
    "showTime": "2024-12-25T19:30:00",
    "seatNumbers": ["A12", "A13"],
    "customerEmail": "tuananh@email.com",
    "totalPrice": 240000
  }'
```

---

## 4. Kết quả chạy thử (Expected Output)

Gộp log của cả 3 service theo thứ tự thời gian:

```text
[MovieBookingService] Created booking CIN-2024-789. CorrelationID: 550e8400-e29b-41d4-a716-446655440000
[SeatAllocationService] Received SeatRequest for CIN-2024-789. CorrelationID: 550e8400-e29b-41d4-a716-446655440000
[SeatAllocationService] Seat reserved: A12, A13. CorrelationID: 550e8400-e29b-41d4-a716-446655440000
[PaymentService] Processing Payment for CIN-2024-789. CorrelationID: 550e8400-e29b-41d4-a716-446655440000
[PaymentService] Payment success: 240000 VND. CorrelationID: 550e8400-e29b-41d4-a716-446655440000
```

`CorrelationID` **giống hệt nhau** ở cả 5 dòng, xuyên suốt 3 service — đúng yêu cầu. (UUID thực tế sẽ khác giá trị ví dụ, nhưng nhất quán trong cùng một luồng.)

---

## 5. Kết luận

Correlation ID được sinh một lần tại điểm khởi đầu và truyền qua header Kafka giúp xâu chuỗi các sự kiện rời rạc thành một hành trình dò được. Đặt ID ở header thay vì payload giữ business schema sạch, cho phép consumer đọc metadata mà không deserialize, và tương thích tự nhiên với các công cụ tracing. Kết hợp MDC, mọi dòng log tự động mang ID mà không cần truyền tay.
