package com.meli.cleancarddefinition.service;


import com.google.common.collect.Lists;
import com.meli.cleancarddefinition.domain.BinSetting;
import com.meli.cleancarddefinition.domain.CardDefinition;
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

    private void executeSql(String sqlScriptPath) {
        long startTime = System.currentTimeMillis();

        log.info("Start execute sql script in pat [{}]", sqlScriptPath);
        try {

            DriverManager.registerDriver(new com.mysql.jdbc.Driver());
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


    private String makeSqlScript(List<Map.Entry<String, IdRepeatedIdsDTO>> cardDefinitionsWithRepeats) {

        try {

            File file = File.createTempFile("binapi-script", ".sql");
            log.info("Script File Path: {}", file.getAbsolutePath());
            FileOutputStream fileOutputStream = new FileOutputStream(file);

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fileOutputStream));

            String lineSeparator = System.getProperty("line.separator");
            for (Map.Entry<String, IdRepeatedIdsDTO> currentKeyValueTuple : cardDefinitionsWithRepeats) {

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


    private Map<String, IdRepeatedIdsDTO> getCardDefinitionsMap() {
        Map<String, IdRepeatedIdsDTO> uniqueIdMap = new HashMap<>();

        Page<CardDefinition> cardDefinitionPage = cardDefinitionRepository.findAll(PageRequest.of(0, PAGE_SIZE));

        int totalPages = cardDefinitionPage.getTotalPages();
        for (int i = 1; i <= totalPages; i++) {


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
        return uniqueIdMap;
    }


    private void executeSql3(String path) {


        String path2 = "/Users/mfigueroa/development/clean-card-definition/src/main/resources/db/migration/binapi-script6560523158312106239.sql";

        try {

            //Process sortProcess = Runtime.getRuntime().exec(new String[]{"mysql", "-u", "binapi", "-pbinapi_dev", "-h", "localhost", "-P", "3306", "binapi", "<", path2});
            Process sortProcess = Runtime.getRuntime().exec("mysql -u binapi -pbinapi_dev -h localhost -P 3306 binapi < /Users/mfigueroa/development/clean-card-definition/src/main/resources/db/migration/binapi-script6560523158312106239.sql");
            //Process sortProcess = new ProcessBuilder(Arrays.asList("mysql", "-u", "binapi", "-pbinapi_dev","-h","localhost","-P","3306","binapi", "<", path2)).start();
            //int exitValue = sortProcess.waitFor();
            /// sortProcess.exitValue()
            int exit = sortProcess.waitFor();

            if (sortProcess.exitValue() != 0) {
/*

                Process exec = Runtime.getRuntime().exec(new String[]{"mysql", "-u", "binapi", "-pbinapi_dev", "show databases"});
                int i = exec.waitFor();
*/


                InputStream stdout = sortProcess.getInputStream();
                InputStream stderr = sortProcess.getErrorStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
                String line1 = null;
                while ((line1 = reader.readLine()) != null) {
                    System.out.println(line1);
                }
                BufferedReader errorred = new BufferedReader(new InputStreamReader(stderr));
                while ((line1 = errorred.readLine()) != null) {
                    System.out.println(line1);
                }

                throw new RuntimeException("Error Impacting Dump");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }



       /* flyway.setLocations(s);
        int resultCode = flyway.migrate();
        log.info("ResultCode: [{}}", resultCode);*/
    }

}
