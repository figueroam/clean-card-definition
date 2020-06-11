package com.meli.cleancarddefinition.repository;

import com.meli.cleancarddefinition.domain.CardDefinition;
import org.springframework.data.repository.PagingAndSortingRepository;

import java.util.List;

public interface CardDefinitionRepository extends PagingAndSortingRepository<CardDefinition, Long> {
    List<CardDefinition> findAllByIdIn(List<Long> ids);
}


