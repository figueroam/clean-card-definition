package com.meli.cleancarddefinition.repository;

import com.meli.cleancarddefinition.domain.BinSetting;
import org.springframework.data.repository.PagingAndSortingRepository;

import java.util.List;

public interface BinSettingRepository extends PagingAndSortingRepository<BinSetting, Long>   {
    List<BinSetting> findAllByCardDefinitionIdIn(List<Long> cardDefinitionId);

}


