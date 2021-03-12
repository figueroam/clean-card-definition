package com.meli.cleancarddefinition.service;


import ch.qos.logback.core.net.SyslogOutputStream;
import com.google.common.collect.Lists;
import com.meli.cleancarddefinition.domain.BinSetting;
import com.meli.cleancarddefinition.domain.CardDefinition;
import com.meli.cleancarddefinition.domain.PaymentMethod;
import com.meli.cleancarddefinition.dto.IdRepeatedIdsDTO;
import com.meli.cleancarddefinition.repository.BinSettingRepository;
import com.meli.cleancarddefinition.repository.CardDefinitionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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

    @Value("${spring.datasource.url}")
    private String dataSourceUrl;

    @Value("${spring.datasource.username}")
    private String dataSourceUsername;

    @Value("${spring.datasource.password}")
    private String dataSourcePassword;


    @Transactional
    public void cleanCardDefinition() throws IOException {
        log.info("Start Clean CardInfo Job");

        long startTime = System.currentTimeMillis();

        Map<String, IdRepeatedIdsDTO> uniqueIdMap = getCardDefinitionsMap();

        List<Map.Entry<String, IdRepeatedIdsDTO>> cardDefinitionsWithRepeateds = deleteEntriesWithoutRepeats(uniqueIdMap);

        //uniqueIdMap It's a very large object so I proceed to delete it after using it
        uniqueIdMap = null;
        System.gc();

        String sqlPath = makeSqlScript(cardDefinitionsWithRepeateds);

        executeSql(sqlPath);

        long endTime = System.currentTimeMillis();

        log.info("Finish job in {}ms", endTime - startTime);
    }

    private Map<String, IdRepeatedIdsDTO> getCardDefinitionsMap() {
        long startTime = System.currentTimeMillis();

        log.info("Start to make CardDefinitionsMap");
        Map<String, IdRepeatedIdsDTO> uniqueIdMap = new HashMap<>();

        Page<CardDefinition> cardDefinitionPage = cardDefinitionRepository.findAll(PageRequest.of(0, PAGE_SIZE));

        int totalPages = cardDefinitionPage.getTotalPages();
        for (int i = 1; i <= totalPages; i++) {


            if (cardDefinitionPage.getTotalElements() > 0) {
                List<CardDefinition> cardDefinitions = cardDefinitionPage.getContent();

                for (CardDefinition currentCardDefinition : cardDefinitions) {

                    List<PaymentMethod> paymentMethods = currentCardDefinition.getPaymentMethods();

                    String issuerId = currentCardDefinition.getIssuerId() != null ? currentCardDefinition.getIssuerId().toString() : "null";
                    String brandId = currentCardDefinition.getBrandId() != null ? currentCardDefinition.getBrandId().toString() : "null";
                    String cardTypeId = currentCardDefinition.getCardTypeId() != null ? currentCardDefinition.getCardTypeId().toString() : "null";
                    String segmentId = currentCardDefinition.getSegmentId() != null ? currentCardDefinition.getSegmentId().toString() : "null";
                    String pmKey = paymentMethods.isEmpty() ? "null" : paymentMethods.stream().map(PaymentMethod::getId).collect(Collectors.joining("_"));

                    String keyMap = String.format("%s-%s-%s-%s-%s", issuerId, brandId, cardTypeId, segmentId, pmKey);
                    System.out.println(keyMap);

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
        long endTime = System.currentTimeMillis();
        log.info("Finish make the map in [{}]ms", endTime - startTime);
        return uniqueIdMap;
    }

    private String makeSqlScript(List<Map.Entry<String, IdRepeatedIdsDTO>> cardDefinitionsWithRepeats) {

        try {

            File file = File.createTempFile("binapi-script", ".sql");
            log.info("Script File Path: {}", file.getAbsolutePath());
            FileOutputStream fileOutputStream = new FileOutputStream(file);

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fileOutputStream));

            int size = cardDefinitionsWithRepeats.size();
            int i = 1;

            String lineSeparator = System.getProperty("line.separator");
            for (Map.Entry<String, IdRepeatedIdsDTO> currentKeyValueTuple : cardDefinitionsWithRepeats) {
                log.info("Writing sql itration {} of {}", i, size);

                IdRepeatedIdsDTO idRepeatedIdsDTO = currentKeyValueTuple.getValue();
                List<Long> ids = idRepeatedIdsDTO.getIds();
                List<List<Long>> partitions = Lists.partition(ids, SQL_IN_STATEMENT_SIZE);
                for (List<Long> currentIdList : partitions) {

                    List<BinSetting> binSettingList = binSettingRepository.findAllByCardDefinitionIdIn(currentIdList);
                    for (BinSetting currentBinSetting : binSettingList) {
                        bw.write(String.format("UPDATE bin_settings SET card_definition_id = %s WHERE id = %s; %s", currentKeyValueTuple.getValue().getId(), currentBinSetting.getId(), lineSeparator));
                    }


                    bw.write(String.format("DELETE FROM card_definitions WHERE id in (%s); %s", currentIdList.stream().map(String::valueOf).collect(Collectors.joining(",")), lineSeparator));
                }
                i++;

            }
            bw.close();
            return file.getAbsolutePath();

        } catch (IOException e) {
            log.error("Error writing SQL File", e);
            throw new RuntimeException("Error writing SQL File");
        }


    }

    private List<Map.Entry<String, IdRepeatedIdsDTO>> deleteEntriesWithoutRepeats(Map<String, IdRepeatedIdsDTO> uniqueIdMap) {
        return uniqueIdMap.entrySet().stream().filter(x -> !x.getValue().getIds().isEmpty()).collect(Collectors.toList());
    }

    private void executeSql(String sqlScriptPath) {
        long startTime = System.currentTimeMillis();

        log.info("Start execute sql script in pat [{}]", sqlScriptPath);
        try {

            DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
            Connection con = DriverManager.getConnection(dataSourceUrl, dataSourceUsername, dataSourcePassword);
            log.info("Connection established......");
            ScriptRunner sr = new ScriptRunner(con);
            Reader reader = new BufferedReader(new FileReader(sqlScriptPath));
            sr.runScript(reader);

        } catch (SQLException | FileNotFoundException e) {
            log.error("Error trying to execute sql script", e);
        }
        long finishTime = System.currentTimeMillis();

        log.info("Finish execute sql script in [{}] ms", finishTime - startTime);
    }


}
