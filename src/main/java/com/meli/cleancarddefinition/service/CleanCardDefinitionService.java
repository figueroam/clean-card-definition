package com.meli.cleancarddefinition.service;


import com.google.common.collect.Lists;
import com.meli.cleancarddefinition.domain.BinSetting;
import com.meli.cleancarddefinition.domain.CardDefinition;
import com.meli.cleancarddefinition.dto.IdRepeatedIdsDTO;
import com.meli.cleancarddefinition.repository.BinSettingRepository;
import com.meli.cleancarddefinition.repository.CardDefinitionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class CleanCardDefinitionService {


    private final CardDefinitionRepository cardDefinitionRepository;
    private final BinSettingRepository binSettingRepository;

    private static final int PAGE_SIZE = 1000;
    private static final int SQL_IN_STATEMENT_SIZE = 999;


    @Transactional
    public void cleanCardDefinition() {

        log.info("Start Clean CardInfo Job");
        long startTime = System.currentTimeMillis();

        Map<String, IdRepeatedIdsDTO> uniqueIdMap = new HashMap<>();


        Page<CardDefinition> cardDefinitionPage = cardDefinitionRepository.findAll(PageRequest.of(0, PAGE_SIZE));

        int totalPages = cardDefinitionPage.getTotalPages();
        for (int i = 1; i <= totalPages; i++) {

            log.info("Geting Page [{}] of card-card_definitions table", i);

            if (cardDefinitionPage.getTotalElements() > 0) {
                List<CardDefinition> cardDefinitions = cardDefinitionPage.getContent();

                for (CardDefinition currentCardDefinition : cardDefinitions) {
                    String keyMap = String.format("%s-%s-%s-%s", currentCardDefinition.getIssuerId(), currentCardDefinition.getBrandId(), currentCardDefinition.getCardTypeId(), currentCardDefinition.getSegmentId());

                    if (uniqueIdMap.get(keyMap) == null) {
                        uniqueIdMap.put(keyMap, IdRepeatedIdsDTO.builder()
                                .id(currentCardDefinition.getId())
                                .ids(new ArrayList<>())
                                .build());
                    } else {
                        uniqueIdMap.get(keyMap).getIds().add(currentCardDefinition.getId());
                    }
                }
            }
            cardDefinitionPage = cardDefinitionRepository.findAll(PageRequest.of(i, PAGE_SIZE));

        }

        List<Map.Entry<String, IdRepeatedIdsDTO>> cardDefinitionsWithRepeateds = uniqueIdMap.entrySet().stream().filter(x -> !x.getValue().getIds().isEmpty()).collect(Collectors.toList());
        uniqueIdMap=null;
        System.gc();
        int cardDefinitionsWithRepeatedsSize = cardDefinitionsWithRepeateds.size();
        log.info("Cant of CardDefinitions with equal fields of issuer-id, brand-id, card-type-id and segment-id is [{}]", cardDefinitionsWithRepeatedsSize);
        int j = 0;
        for (Map.Entry<String, IdRepeatedIdsDTO> currentKeyValueTuple : cardDefinitionsWithRepeateds) {
            log.info("Iteration number: {} of {}", j, cardDefinitionsWithRepeatedsSize);
            j++;

            IdRepeatedIdsDTO idRepeatedIdsDTO = currentKeyValueTuple.getValue();
            List<Long> ids = idRepeatedIdsDTO.getIds();
            List<List<Long>> partitions = Lists.partition(ids, SQL_IN_STATEMENT_SIZE);
            for (List<Long> currentIdList : partitions) {

                List<BinSetting> binSettingList = binSettingRepository.findAllByCardDefinitionIdIn(currentIdList);
                binSettingList.forEach(x -> x.setCardDefinitionId(currentKeyValueTuple.getValue().getId()));
                binSettingRepository.saveAll(binSettingList);


                List<CardDefinition> allByIdIn = cardDefinitionRepository.findAllByIdIn(currentIdList);
                cardDefinitionRepository.deleteAll(allByIdIn);
            }


        }
        long endTime = System.currentTimeMillis();

        log.info("Finish job in {}ms", endTime - startTime);
    }

}
