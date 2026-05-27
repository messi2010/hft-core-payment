package com.payments.tbjava.api.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public record CreateTransfersRequest(
        @NotEmpty @Valid List<TransferDto> transfers
) {
    public record TransferDto(
            String id,                 // u128 as decimal string or UUID
            String debitAccountId,
            String creditAccountId,
            long amount,
            String pendingId,          // "" / "0" if none
            long userData64,
            int userData32,
            int timeoutSeconds,
            int ledger,
            short code,
            short flags
    ) {}
}
