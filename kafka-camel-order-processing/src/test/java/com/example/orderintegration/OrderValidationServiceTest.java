package com.example.orderintegration;

import com.example.orderintegration.model.OrderEvent;
import com.example.orderintegration.service.OrderValidationService;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;

class OrderValidationServiceTest {
    private final OrderValidationService service = new OrderValidationService();

    @Test
    void validOrderIsAccepted() {
        var o = new OrderEvent("O1","C1",new BigDecimal("10"),"EUR","P1",1);
        assertNull(service.validate(o));
    }

    @Test
    void quantityAboveThreeIsRejected() {
        var o = new OrderEvent("O1","C1",new BigDecimal("10"),"EUR","P1",4);
        assertEquals("quantity exceeds the allowed maximum of 3", service.validate(o));
    }
}
