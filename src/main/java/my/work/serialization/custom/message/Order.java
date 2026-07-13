package my.work.serialization.custom.message;

import lombok.Builder;

@Builder
public record Order(String customerName, String product, int quantity) {
}
