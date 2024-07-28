package com.maxrybalkin91.transactions.service;

import com.maxrybalkin91.transactions.model.Item;
import com.maxrybalkin91.transactions.repository.ItemRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ItemService {
    private final ItemRepository itemRepository;

    @Transactional
    public Item saveReadCommited(Item Item) {
        return itemRepository.save(Item);
    }

    @Transactional(isolation = Isolation.REPEATABLE_READ)
    public Item saveRepeatableRead(Item Item) {
        return itemRepository.save(Item);
    }

    @Transactional(isolation = Isolation.SERIALIZABLE)
    public Item saveSerializable(Item Item) {
        return itemRepository.save(Item);
    }
}
