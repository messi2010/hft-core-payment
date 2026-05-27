package com.payments.ledger.api.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public record CreateAccountsRequest(
        @NotEmpty @Valid List<AccountDto> accounts
) {
    public record AccountDto(
            String id,           // u128 as decimal string or UUID
            int ledger,
            short code,
            short flags,
            long userData64,
            int userData32
    ) {}
}
