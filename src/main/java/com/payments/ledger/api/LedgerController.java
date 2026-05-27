package com.payments.ledger.api;

import com.payments.ledger.api.dto.CreateAccountsRequest;
import com.payments.ledger.api.dto.CreateTransfersRequest;
import com.payments.ledger.domain.Account;
import com.payments.ledger.domain.AccountSnapshot;
import com.payments.ledger.domain.CreateAccountResult;
import com.payments.ledger.domain.CreateTransferResult;
import com.payments.ledger.domain.LedgerBalance;
import com.payments.ledger.domain.Transfer;
import com.payments.ledger.domain.UInt128;
import com.payments.ledger.engine.CapacityExceededException;
import com.payments.ledger.engine.LedgerEngine;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Reactive (Spring WebFlux / Netty) HTTP layer. Handlers run on the event loop
 * and never block: each maps the request, hands the batch to the engine (a
 * non-blocking ring-buffer publish), and returns a {@link Mono} bridged from the
 * engine's {@code CompletableFuture}. Backpressure (a full ring) surfaces as
 * {@link CapacityExceededException} -> HTTP 429.
 */
@RestController
@RequestMapping("/v1")
public class LedgerController {

    private final LedgerEngine engine;

    public LedgerController(LedgerEngine engine) {
        this.engine = engine;
    }

    @PostMapping("/accounts")
    public Mono<ResponseEntity<List<String>>> createAccounts(
            @Valid @RequestBody CreateAccountsRequest req) {
        List<Account> domain = req.accounts().stream()
                .map(a -> new Account(UInt128.parse(a.id()), a.ledger(), a.code(), a.flags(),
                        a.userData64(), a.userData32(), 0))
                .toList();
        return Mono.fromFuture(engine.createAccounts(domain))
                .map(results -> ResponseEntity.ok(
                        results.stream().map(CreateAccountResult::name).toList()));
    }

    @PostMapping("/transfers")
    public Mono<ResponseEntity<List<String>>> createTransfers(
            @Valid @RequestBody CreateTransfersRequest req) {
        List<Transfer> domain = req.transfers().stream()
                .map(t -> new Transfer(UInt128.parse(t.id()),
                        UInt128.parse(t.debitAccountId()), UInt128.parse(t.creditAccountId()),
                        t.amount(), UInt128.parse(t.pendingId()), t.userData64(), t.userData32(),
                        t.timeoutSeconds(), t.ledger(), t.code(), t.flags(), 0))
                .toList();
        return Mono.fromFuture(engine.createTransfers(domain))
                .map(results -> ResponseEntity.ok(
                        results.stream().map(CreateTransferResult::name).toList()));
    }

    @GetMapping("/accounts/{id}")
    public Mono<ResponseEntity<AccountSnapshot>> getAccount(@PathVariable String id) {
        return Mono.fromFuture(engine.lookupAccount(UInt128.parse(id)))
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping("/transfers/{id}")
    public Mono<ResponseEntity<Transfer>> getTransfer(@PathVariable String id) {
        return Mono.fromFuture(engine.lookupTransfer(UInt128.parse(id)))
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /** Account transfer history, newest first. {@code limit} caps the result (default 100, max 1000). */
    @GetMapping("/accounts/{id}/transfers")
    public Mono<ResponseEntity<List<Transfer>>> getAccountTransfers(
            @PathVariable String id,
            @RequestParam(defaultValue = "100") int limit) {
        int capped = Math.max(1, Math.min(limit, 1000));
        return Mono.fromFuture(engine.getAccountTransfers(UInt128.parse(id), capped))
                .map(ResponseEntity::ok);
    }

    /** Trial balance per ledger — posted debits should equal posted credits. */
    @GetMapping("/trial-balance")
    public Mono<ResponseEntity<List<LedgerBalance>>> trialBalance() {
        return Mono.fromFuture(engine.trialBalance()).map(ResponseEntity::ok);
    }

    /** Ring buffer full -> tell the client to back off and retry. */
    @ExceptionHandler(CapacityExceededException.class)
    public ResponseEntity<String> onCapacityExceeded(CapacityExceededException e) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(e.getMessage());
    }
}
