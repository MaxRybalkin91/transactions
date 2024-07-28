package com.maxrybalkin91.transactions.repository;

import com.maxrybalkin91.transactions.model.Account;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Repository
public interface AccountRepository extends JpaRepository<Account, Long> {
    @Override
    @Transactional
    Optional<Account> findById(Long id);
}
