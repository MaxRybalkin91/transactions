package com.maxrybalkin91.transactions.service;

import com.maxrybalkin91.transactions.model.Account;
import com.maxrybalkin91.transactions.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class AccountService {
    private final AccountRepository accountRepository;

    @Transactional
    public Account saveReadCommited(Account account) {
        return accountRepository.save(account);
    }

    @Transactional(isolation = Isolation.REPEATABLE_READ)
    public Account saveRepeatableRead(Account account) {
        return accountRepository.save(account);
    }

    @Transactional(isolation = Isolation.SERIALIZABLE)
    public Account saveSerializable(Account account) {
        return accountRepository.save(account);
    }
}
