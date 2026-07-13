package my.work.serialization.custom.message;

import lombok.Builder;

@Builder
public record CustomOrder(String customerName, String product, int quantity) {
}
