package com.payments.ledger.api;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.payments.ledger.domain.UInt128;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers ledger-specific Jackson customizations. Spring Boot auto-applies any
 * {@link Module} bean to the global {@code ObjectMapper}.
 */
@Configuration
public class LedgerJacksonConfig {

    @Bean
    public Module ledgerJacksonModule() {
        SimpleModule m = new SimpleModule("LedgerIds");
        m.addSerializer(UInt128.class, new UInt128JsonSerializer());
        return m;
    }
}
