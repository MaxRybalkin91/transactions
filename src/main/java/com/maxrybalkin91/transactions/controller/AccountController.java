package com.maxrybalkin91.transactions.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AccountController {

    @GetMapping
    public String getHello() {
        return "Hello World!";
    }
}
