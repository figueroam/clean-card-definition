package com.meli.cleancarddefinition.controller;

import com.meli.cleancarddefinition.service.CleanCardDefinitionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;


@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping(value = "/clean-card-definition")
public class CleanCardDefinitionController {

    private final CleanCardDefinitionService cleanCardDefinitionService;

    @PostMapping
    public ResponseEntity<?> cleanCardDefinitionTable() throws IOException {
        log.info("Executing cleanCardDefinition process");
        cleanCardDefinitionService.cleanCardDefinition();
        return ResponseEntity.ok().build();
    }
}
