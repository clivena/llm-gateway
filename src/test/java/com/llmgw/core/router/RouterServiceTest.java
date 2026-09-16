package com.llmgw.core.router;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RouterServiceTest {

    private final RouterService router = new RouterService();

    @Test
    void costFirstPicksCheapest() {
        assertEquals("CHEAP", router.pick(RoutingPolicy.COST_FIRST).tier());
    }

    @Test
    void qualityFirstPicksStrongest() {
        assertEquals("STRONG", router.pick(RoutingPolicy.QUALITY_FIRST).tier());
    }

    @Test
    void cheapModelEscalatesWhenFormatInvalid() {
        ModelTier cheap = router.pick(RoutingPolicy.COST_FIRST);
        assertTrue(router.shouldEscalate(cheap, "bad", false));
        assertEquals("STRONG", router.escalateFrom(cheap).tier());
    }
}
